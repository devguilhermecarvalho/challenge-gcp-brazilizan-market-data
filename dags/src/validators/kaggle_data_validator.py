import os
from google.cloud import storage
from datetime import datetime
from src.extractors.kaggle_data_extractor import KaggleDataExtractor

class KaggleDataValidator:
    def __init__(self, config=None):
        # Inicializar o cliente do Google Cloud Storage
        self.storage_client = storage.Client()
        self.config = config or {}
        self.bucket_name = self.config.get('kaggle', {}).get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('kaggle', {}).get('folder', 'bronze')
        self.extractor = KaggleDataExtractor(config=self.config)

    def data_exists(self, dataset_id):
        # Obter a data da última atualização do dataset no Kaggle
        metadata = self.extractor.get_dataset_metadata(dataset_id)
        if not metadata:
            print(f"Não foi possível obter metadados para o dataset {dataset_id}.")
            return False

        last_updated = metadata['lastUpdated']
        update_date = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d')

        # Construir o caminho no GCS
        dataset_name = dataset_id.split('/')[-1]
        blob_prefix = f"{self.folder}/{dataset_name}/{update_date}/"

        bucket = self.storage_client.bucket(self.bucket_name)
        blobs = list(bucket.list_blobs(prefix=blob_prefix))
        if blobs:
            print(f"Dados para o dataset {dataset_id} já existem no GCS.")
            return True
        else:
            print(f"Dados para o dataset {dataset_id} não existem no GCS.")
            return False

    def validate_datasets(self, dataset_ids):
        datasets_exist = {}
        for dataset_id in dataset_ids:
            datasets_exist[dataset_id] = self.data_exists(dataset_id)
        return datasets_exist
