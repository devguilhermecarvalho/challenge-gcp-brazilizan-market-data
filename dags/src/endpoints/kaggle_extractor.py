import os
import requests
import json
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime
from google.cloud import storage

class KaggleDatasetDownloader:
    def __init__(self, kaggle_json_path='kaggle.json', config=None):
        # Load Kaggle credentials from the JSON file
        with open(kaggle_json_path, 'r') as file:
            self.kaggle_credentials = json.load(file)
        self.username = self.kaggle_credentials['username']
        self.key = self.kaggle_credentials['key']
        
        # Initialize the Kaggle API
        self.api = KaggleApi()
        self.api.authenticate()

        # GCS Client
        self.storage_client = storage.Client()

        # Configurations
        self.config = config or {}
        self.bucket_name = self.config.get('kaggle', {}).get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('kaggle', {}).get('folder', 'bronze')
    
    def get_dataset_metadata(self, dataset_id):
        # Configure the dataset metadata URL
        metadata_url = f'https://www.kaggle.com/api/v1/datasets/view/{dataset_id}'
        # Make the request to get dataset metadata
        response = requests.get(metadata_url, auth=(self.username, self.key))
        # Check if the request was successful
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting metadata for {dataset_id}: {response.status_code} - {response.text}")
            return None
    
    def download_dataset(self, dataset_id, base_folder):
        # Download the dataset
        self.api.dataset_download_files(dataset_id, path=base_folder, unzip=True)
    
    def verify_download(self, expected_size, base_folder):
        # Verify the size of the downloaded files
        downloaded_file_size = sum(
            os.path.getsize(os.path.join(base_folder, f)) for f in os.listdir(base_folder)
        )
        print("Expected size (in bytes):", expected_size)
        print("Downloaded file size (in bytes):", downloaded_file_size)
        
        # Compare sizes and display the result
        if downloaded_file_size == expected_size:
            print("Download complete: the size of downloaded files matches the expected size.")
        else:
            print("Warning: the size of downloaded files does not match the expected size.")
    
    def upload_to_gcs(self, local_file_path, gcs_blob_name):
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"Uploaded {local_file_path} to gs://{self.bucket_name}/{gcs_blob_name}")
    
    def process_dataset(self, dataset_id):
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return
        
        # Get relevant information
        title = metadata['title']
        description = metadata['subtitle']
        last_updated = metadata['lastUpdated']
        expected_size = metadata['totalBytes']
        
        # Convert the last updated date to a usable format
        update_date = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d')
    
        # Configure folders and files based on the update date
        dataset_name = dataset_id.split('/')[-1]
        base_folder = f'/tmp/{dataset_name}_{update_date}'
        os.makedirs(base_folder, exist_ok=True)
        
        # Download the dataset
        self.download_dataset(dataset_id, base_folder)
        
        # Verify the download
        self.verify_download(expected_size, base_folder)
        
        # Rename the main file
        self.rename_main_file(base_folder, update_date, dataset_name)

        # Upload files to GCS
        self.upload_files_to_gcs(base_folder, dataset_name, update_date)

        # Clean up local files
        self.cleanup_local_files(base_folder)
    
    def rename_main_file(self, base_folder, update_date, dataset_name):
        # Rename the main file with the last update date
        main_file_name = f'{dataset_name}_{update_date}.csv'
        main_file_path = os.path.join(base_folder, main_file_name)
        
        # Save one of the downloaded files as an example
        csv_files = [file for file in os.listdir(base_folder) if file.endswith(".csv")]
        if csv_files:
            os.rename(os.path.join(base_folder, csv_files[0]), main_file_path)
            print(f"Dataset saved as: {main_file_path}")
        else:
            print("No CSV file found to rename.")
    
    def upload_files_to_gcs(self, base_folder, dataset_name, update_date):
        # Upload all files in the base_folder to GCS
        for filename in os.listdir(base_folder):
            local_file_path = os.path.join(base_folder, filename)
            gcs_blob_name = f"{self.folder}/{dataset_name}/{update_date}/{filename}"
            self.upload_to_gcs(local_file_path, gcs_blob_name)
    
    def cleanup_local_files(self, base_folder):
        # Remove local files after upload
        for filename in os.listdir(base_folder):
            os.remove(os.path.join(base_folder, filename))
        os.rmdir(base_folder)