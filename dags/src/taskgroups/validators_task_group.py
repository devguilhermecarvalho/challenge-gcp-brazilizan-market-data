from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from src.validators.bigquery_validator import BigQueryManager
from src.validators.cloud_storage_validator import CloudStorageValidator

def validator_task_group():
    with TaskGroup("validators", tooltip="Validator Tasks") as group:
        @task
        def validate_buckets_and_datasets():
            # Validate or create GCP buckets and datasets
            cloud_storage_validator = CloudStorageValidator(config_path="configs/google_cloud.yml")
            cloud_storage_validator.validate_or_create_buckets_and_tags()

            bigquery_manager = BigQueryManager(config_path="configs/google_cloud.yml")
            bigquery_manager.setup_datasets()

        validate_buckets_and_datasets()

    return group
