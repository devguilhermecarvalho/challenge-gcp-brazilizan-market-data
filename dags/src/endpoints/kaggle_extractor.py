import os
import requests
import json
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime

class KaggleDatasetDownloader:
    def __init__(self, kaggle_json_path='kaggle.json'):
        # Carregar as credenciais do Kaggle a partir do arquivo JSON
        with open(kaggle_json_path, 'r') as file:
            self.kaggle_credentials = json.load(file)
        self.username = self.kaggle_credentials['username']
        self.key = self.kaggle_credentials['key']
        
        # Inicializar a API do Kaggle
        self.api = KaggleApi()
        self.api.authenticate()
    
    def get_dataset_metadata(self, dataset_id):
        # Configurar a URL do metadado do dataset
        metadata_url = f'https://www.kaggle.com/api/v1/datasets/view/{dataset_id}'
        # Fazer a requisição para obter metadados do dataset
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
        print("Tamanho do arquivo baixado (em bytes):", downloaded_file_size)
        
        # Comparar tamanhos e exibir o resultado
        if downloaded_file_size == expected_size:
            print("Download completo: o tamanho dos arquivos baixados corresponde ao tamanho esperado.")
        else:
            print("Aviso: o tamanho dos arquivos baixados não corresponde ao tamanho esperado.")
    
    def rename_main_file(self, base_folder, update_date, dataset_name):
        # Nomear o arquivo principal com a data da última atualização
        main_file_name = f'{dataset_name}_{update_date}.csv'
        main_file_path = os.path.join(base_folder, main_file_name)
        
        # Salvar um dos arquivos baixados como exemplo
        csv_files = [file for file in os.listdir(base_folder) if file.endswith(".csv")]
        if csv_files:
            os.rename(os.path.join(base_folder, csv_files[0]), main_file_path)
            print(f"Dataset salvo em: {main_file_path}")
        else:
            print("Nenhum arquivo CSV encontrado para renomear.")
    
    def process_dataset(self, dataset_id):
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return
        
        # Obter informações de interesse
        title = metadata['title']
        description = metadata['subtitle']
        last_updated = metadata['lastUpdated']
        expected_size = metadata['totalBytes']
        
        # Converter a data de última atualização para um formato utilizável
        update_date = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d')
    
        # Configurar as pastas e arquivos com base na data de atualização
        dataset_name = dataset_id.split('/')[-1]
        base_folder = f'../data/{dataset_name}_{update_date}'
        os.makedirs(base_folder, exist_ok=True)
        
        # Baixar o dataset
        self.download_dataset(dataset_id, base_folder)
        
        # Verificar o download
        self.verify_download(expected_size, base_folder)
        
        # Renomear o arquivo principal
        self.rename_main_file(base_folder, update_date, dataset_name)

if __name__ == "__main__":
    dataset_ids = ['olistbr/marketing-funnel-olist', 'olistbr/brazilian-ecommerce']
    downloader = KaggleDatasetDownloader()
    for dataset_id in dataset_ids:
        downloader.process_dataset(dataset_id)
