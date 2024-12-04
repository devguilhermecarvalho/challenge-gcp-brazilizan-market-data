import os
from google.cloud import storage

class KaggleDataUploader:
    def __init__(self, config=None):
        # Inicializar o cliente do Google Cloud Storage
        self.storage_client = storage.Client()
        self.config = config or {}
        self.bucket_name = self.config.get('kaggle', {}).get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('kaggle', {}).get('folder', 'bronze')

    def upload_file_to_gcs(self, local_file_path, dataset_name, update_date):
        # Enviar o arquivo compactado para o GCS
        bucket = self.storage_client.bucket(self.bucket_name)
        filename = os.path.basename(local_file_path)
        gcs_blob_name = f"{self.folder}/{dataset_name}/{update_date}/{filename}"
        blob = bucket.blob(gcs_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"Enviado {local_file_path} para gs://{self.bucket_name}/{gcs_blob_name}")

    def cleanup_local_files(self, base_folder):
        # Remover arquivos locais após o upload
        for filename in os.listdir(base_folder):
            os.remove(os.path.join(base_folder, filename))
        os.rmdir(base_folder)
