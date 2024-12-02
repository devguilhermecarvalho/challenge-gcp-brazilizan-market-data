import os
import requests
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime
from google.cloud import storage

class KaggleDatasetDownloader:
    def __init__(self, config=None):
        # Carregar as credenciais do Kaggle das variáveis de ambiente
        self.username = os.environ.get('KAGGLE_USERNAME')
        self.key = os.environ.get('KAGGLE_KEY')
        if not self.username or not self.key:
            raise ValueError("As credenciais do Kaggle não foram encontradas nas variáveis de ambiente.")

        # Definir as credenciais no ambiente para que a API do Kaggle possa usá-las
        os.environ['KAGGLE_USERNAME'] = self.username
        os.environ['KAGGLE_KEY'] = self.key

        # Inicializar a API do Kaggle
        self.api = KaggleApi()
        self.api.authenticate()

        # Cliente do Google Cloud Storage
        self.storage_client = storage.Client()

        # Configurações
        self.config = config or {}
        self.bucket_name = self.config.get('kaggle', {}).get('bucket_name', 'kaggle_landing_zone')
        self.folder = self.config.get('kaggle', {}).get('folder', 'bronze')
    
    def get_dataset_metadata(self, dataset_id):
        # Configurar a URL de metadados do dataset
        metadata_url = f'https://www.kaggle.com/api/v1/datasets/view/{dataset_id}'
        # Fazer a requisição para obter os metadados do dataset
        response = requests.get(metadata_url, auth=(self.username, self.key))
        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter metadados para {dataset_id}: {response.status_code} - {response.text}")
            return None
    
    def download_dataset(self, dataset_id, base_folder):
        # Baixar o dataset
        self.api.dataset_download_files(dataset_id, path=base_folder, unzip=True)
    
    def verify_download(self, expected_size, base_folder):
        # Verificar o tamanho dos arquivos baixados
        downloaded_file_size = sum(
            os.path.getsize(os.path.join(base_folder, f)) for f in os.listdir(base_folder)
        )
        print("Tamanho esperado (em bytes):", expected_size)
        print("Tamanho dos arquivos baixados (em bytes):", downloaded_file_size)
        
        # Comparar os tamanhos e exibir o resultado
        if downloaded_file_size == expected_size:
            print("Download completo: o tamanho dos arquivos baixados corresponde ao tamanho esperado.")
        else:
            print("Aviso: o tamanho dos arquivos baixados não corresponde ao tamanho esperado.")
    
    def upload_to_gcs(self, local_file_path, gcs_blob_name):
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"Enviado {local_file_path} para gs://{self.bucket_name}/{gcs_blob_name}")
    
    def process_dataset(self, dataset_id):
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return
        
        # Obter informações relevantes
        title = metadata['title']
        description = metadata['subtitle']
        last_updated = metadata['lastUpdated']
        expected_size = metadata['totalBytes']
        
        # Converter a data de atualização para um formato utilizável
        update_date = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d')
    
        # Configurar pastas e arquivos com base na data de atualização
        dataset_name = dataset_id.split('/')[-1]
        base_folder = f'/tmp/{dataset_name}_{update_date}'
        os.makedirs(base_folder, exist_ok=True)
        
        # Baixar o dataset
        self.download_dataset(dataset_id, base_folder)
        
        # Verificar o download
        self.verify_download(expected_size, base_folder)
        
        # Renomear o arquivo principal
        self.rename_main_file(base_folder, update_date, dataset_name)

        # Enviar arquivos para o GCS
        self.upload_files_to_gcs(base_folder, dataset_name, update_date)

        # Limpar arquivos locais
        self.cleanup_local_files(base_folder)
    
    def rename_main_file(self, base_folder, update_date, dataset_name):
        # Renomear o arquivo principal com a data da última atualização
        main_file_name = f'{dataset_name}_{update_date}.csv'
        main_file_path = os.path.join(base_folder, main_file_name)
        
        # Salvar um dos arquivos baixados como exemplo
        csv_files = [file for file in os.listdir(base_folder) if file.endswith(".csv")]
        if csv_files:
            os.rename(os.path.join(base_folder, csv_files[0]), main_file_path)
            print(f"Dataset salvo como: {main_file_path}")
        else:
            print("Nenhum arquivo CSV encontrado para renomear.")
    
    def upload_files_to_gcs(self, base_folder, dataset_name, update_date):
        # Enviar todos os arquivos na base_folder para o GCS
        for filename in os.listdir(base_folder):
            local_file_path = os.path.join(base_folder, filename)
            gcs_blob_name = f"{self.folder}/{dataset_name}/{update_date}/{filename}"
            self.upload_to_gcs(local_file_path, gcs_blob_name)
    
    def cleanup_local_files(self, base_folder):
        # Remover arquivos locais após o upload
        for filename in os.listdir(base_folder):
            os.remove(os.path.join(base_folder, filename))
        os.rmdir(base_folder)