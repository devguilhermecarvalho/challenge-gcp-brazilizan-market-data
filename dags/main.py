# dags/main.py
from pendulum import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

from src.validators.bigquery_validator import BigQueryManager
from src.validators.cloud_storage_validator import CloudStorageValidator
from src.endpoints.kaggle_extractor import KaggleDatasetDownloader
from src.validators.validators_config_loader import ConfigLoader

# Parâmetros do DAG
@dag(
    start_date=datetime(2024, 11, 11),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 1},
    tags=["exemplo"]
)
def pipeline_kaggle_extractor():
    @task
    def validate_buckets_and_datasets():
        # Validar ou criar buckets e datasets no GCP
        cloud_storage_validator = CloudStorageValidator(config_path="configs/google_cloud.yml")
        cloud_storage_validator.validate_or_create_buckets_and_tags()

        bigquery_manager = BigQueryManager(config_path="configs/google_cloud.yml")
        bigquery_manager.setup_datasets()

    @task
    def extract_kaggle_datasets():
        config_loader = ConfigLoader(config_path="configs/kaggle.yml")
        kaggle_configs = config_loader.get_kaggle_configs()
        dataset_ids = [dataset['id'] for dataset in kaggle_configs.get('datasets', [])]
        downloader = KaggleDatasetDownloader(kaggle_json_path='kaggle.json', config=kaggle_configs)
        for dataset_id in dataset_ids:
            downloader.process_dataset(dataset_id)

    # Definir o grupo de tarefas de validação
    with TaskGroup("validators", tooltip="Tarefas de Validação") as validate_group:
        validate_buckets_and_datasets()

    # Definir o grupo de tarefas dbt usando Astronomer Cosmos
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
            conn_id="CONNECTION_ID",
            profile_args="",
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

    validate_group >> extract_kaggle_datasets() >> dbt_transform

pipeline_kaggle_extractor()
