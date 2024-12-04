import os
import requests
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime

class KaggleDataExtractor:
    def __init__(self, config=None):
        # Carregar as credenciais do Kaggle das variáveis de ambiente
        self.username = os.environ.get('KAGGLE_USERNAME')
        self.key = os.environ.get('KAGGLE_KEY')

        if not self.username or not self.key:
            raise ValueError("As credenciais do Kaggle não estão definidas nas variáveis de ambiente.")

        # Inicializar a API do Kaggle
        # Solução para o erro FileExistsError
        # Verificar se o diretório de configuração já existe
        config_dir = os.path.join(os.path.expanduser('~'), '.kaggle')
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        # Criar o arquivo kaggle.json com as credenciais, se não existir
        kaggle_json_path = os.path.join(config_dir, 'kaggle.json')
        if not os.path.exists(kaggle_json_path):
            with open(kaggle_json_path, 'w') as f:
                f.write('{"username":"%s","key":"%s"}' % (self.username, self.key))
            os.chmod(kaggle_json_path, 0o600)

        self.api = KaggleApi()
        self.api.authenticate()

        # Configurações
        self.config = config or {}

    def get_dataset_metadata(self, dataset_id):
        metadata_url = f'https://www.kaggle.com/api/v1/datasets/view/{dataset_id}'
        response = requests.get(metadata_url, auth=(self.username, self.key))
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter metadados para {dataset_id}: {response.status_code} - {response.text}")
            return None

    def download_dataset(self, dataset_id, base_folder):
        self.api.dataset_download_files(dataset_id, path=base_folder, unzip=False)

    def verify_download(self, expected_size, file_path):
        # Verificar o tamanho do arquivo baixado (compactado)
        if not os.path.exists(file_path):
            print(f"O arquivo {file_path} não foi encontrado.")
            return False

        downloaded_file_size = os.path.getsize(file_path)
        print("Tamanho esperado (em bytes):", expected_size)
        print("Tamanho do arquivo baixado (em bytes):", downloaded_file_size)

        # Comparar os tamanhos e exibir o resultado
        if downloaded_file_size == expected_size:
            print("Download completo: o tamanho do arquivo baixado corresponde ao tamanho esperado.")
            return True
        else:
            print("Aviso: o tamanho do arquivo baixado não corresponde ao tamanho esperado.")
            return False

    def extract_dataset(self, dataset_id):
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return None

        # Obter informações relevantes
        last_updated = metadata['lastUpdated']
        expected_size = metadata['totalBytes']

        # Converter a data de atualização para um formato utilizável
        update_date = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d')

        # Configurar pastas e arquivos com base na data de atualização
        dataset_name = dataset_id.split('/')[-1]
        base_folder = f'/tmp/{dataset_name}_{update_date}'
        os.makedirs(base_folder, exist_ok=True)

        # Baixar o dataset (compactado)
        self.download_dataset(dataset_id, base_folder)

        # Caminho do arquivo baixado
        file_path = os.path.join(base_folder, f"{dataset_name}.zip")

        # Verificar o download
        if not self.verify_download(expected_size, file_path):
            print(f"Download falhou ou arquivo corrompido para {dataset_id}.")
            return None

        return file_path, dataset_name, update_date
