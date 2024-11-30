from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from src.validators.bigquery_validator import BigQueryManager
from src.validators.cloud_storage_validator import CloudStorageValidator

class ValidatorsTaskGroup:
    def __init__(self):
        self.task_group = self._create_task_group()

    def _create_task_group(self):
        with TaskGroup("validators", tooltip="Validator Tasks") as group:
            @task(task_group=self)
            def validate_buckets_and_datasets():
                
                cloud_storage_validator = CloudStorageValidator(config_path="configs/google_cloud.yml")
                cloud_storage_validator.validate_or_create_buckets_and_tags()

                bigquery_manager = BigQueryManager(config_path="configs/google_cloud.yml")
                bigquery_manager.setup_datasets()

            validate_buckets_and_datasets()