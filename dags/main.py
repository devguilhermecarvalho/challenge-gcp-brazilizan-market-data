from pendulum import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig

from src.validators.bigquery_validator import BigQueryManager
from src.validators.cloud_storage_validator import CloudStorageValidator
from src.endpoints.kaggle_extractor import KaggleDatasetDownloader
from src.validators.validators_config_loader import ConfigLoader

# Parameters of the DAG
@dag(
    start_date=datetime(2024, 11, 11),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["example"]
)
def pipeline_kaggle_extractor():
    @task
    def validate_buckets_and_datasets():
        # Validate or create GCP buckets and datasets
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

    # Define the validator task group
    with TaskGroup("validators", tooltip="Validator Tasks") as validate_group:
        validate_buckets_and_datasets()

    # Define the dbt task group using Astronomer Cosmos
    dbt_tg = DbtTaskGroup(
        group_id='dbt_transformations',
        project_config=ProjectConfig(
            dbt_project_name='your_dbt_project',
            project_dir='/path/to/your/dbt/project',
            profiles_dir='/path/to/your/dbt/profiles',
            profile_name='default',
            target_name='prod',
        ),
        # Adjust the select parameter as needed
        select='state:modified+',
    )

    # Set task dependencies
    validate_group >> extract_kaggle_datasets() >> dbt_tg

pipeline_kaggle_extractor()
