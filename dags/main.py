from pendulum import datetime
from pathlib import Path

import os
import json

from airflow.decorators import dag, task, task_group
from airflow.models import Connection
from airflow import settings

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

@dag(
    start_date=datetime(2024, 11, 11),
    schedule="@daily",
    catchup=False,
)
def pipeline_kaggle_extractor():
    @task_group
    def validate_buckets_and_datasets():
        @task
        def validate_or_create_buckets_and_tags():
            from src.validators.cloud_storage_validator import CloudStorageValidator
            config_loader = "dags/src/configs/google_cloud.yml"
            cloud_storage_validator = CloudStorageValidator(config_path=config_loader)
            cloud_storage_validator.validate_or_create_buckets_and_tags()

        @task
        def setup_datasets():
            from src.validators.bigquery_validator import BigQueryManager
            config_loader = "dags/src/configs/google_cloud.yml"
            bigquery_manager = BigQueryManager(config_path=config_loader)
            bigquery_manager.setup_datasets()

        validate_or_create_buckets_and_tags_task = validate_or_create_buckets_and_tags()
        setup_datasets_task = setup_datasets()

        validate_or_create_buckets_and_tags_task >> setup_datasets_task

    @task
    def extract_kaggle_datasets():
        from src.validators.validators_config_loader import ConfigLoader
        from src.endpoints.kaggle_extractor import KaggleDatasetDownloader

        config_loader = ConfigLoader(config_path="dags/src/configs/kaggle.yml")
        kaggle_configs = config_loader.get_kaggle_configs()
        dataset_ids = [dataset['id'] for dataset in kaggle_configs.get('datasets', [])]

        # Assegure-se de que as credenciais do Kaggle estão definidas nas variáveis de ambiente
        if not os.environ.get('KAGGLE_USERNAME') or not os.environ.get('KAGGLE_KEY'):
            raise ValueError("As credenciais do Kaggle não estão definidas nas variáveis de ambiente.")

        downloader = KaggleDatasetDownloader(config=kaggle_configs)

        for dataset_id in dataset_ids:
            downloader.process_dataset(dataset_id)

    def create_gcp_connection():
        from airflow.models import Connection
        from airflow import settings
        import json

        conn_id = 'google_cloud_default'  # Substitua pelo seu conn_id desejado

        # Verifica se a conexão já existe
        session = settings.Session()
        conn_exists = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .first()
        )

        if not conn_exists:
            # Obtém o caminho para o arquivo de chave a partir da variável de ambiente
            keyfile_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            if not keyfile_path:
                raise ValueError("A variável de ambiente GOOGLE_APPLICATION_CREDENTIALS não está definida.")

            # Carrega o conteúdo do arquivo de chave
            with open(keyfile_path, 'r') as f:
                keyfile_dict = json.load(f)

            conn = Connection(
                conn_id=conn_id,
                conn_type='google_cloud_platform',
                extra=json.dumps({
                    'extra__google_cloud_platform__keyfile_dict': keyfile_dict,
                    'extra__google_cloud_platform__scope': 'https://www.googleapis.com/auth/cloud-platform',
                })
            )
            session.add(conn)
            session.commit()
            session.close()
        else:
            session.close()

    # Cria a conexão do GCP programaticamente usando a chave do ENV GOOGLE_APPLICATION_CREDENTIALS
    create_gcp_connection()

    # Atualize o conn_id para corresponder ao criado
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
            conn_id="google_cloud_default",  # Deve corresponder ao conn_id usado em create_gcp_connection
            profile_args={
                "schema": "your_bigquery_dataset_name",  # Substitua pelo nome do seu dataset no BigQuery
                "project": "your_gcp_project_id",       # Substitua pelo ID do seu projeto GCP
            },
        ),
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(dbt_project_path=Path(__file__).parent / "dbt_transformations"),
        profile_config=profile_config,
        default_args={"owner": "Astro", "retries": 1},
        operator_args={
            "dbt_executable_path": "/usr/local/airflow/.local/bin/dbt"
        }
    )

    validate_buckets_and_datasets_task_group = validate_buckets_and_datasets()

    validate_buckets_and_datasets_task_group >> extract_kaggle_datasets() >> dbt_transform

pipeline_kaggle_extractor()
