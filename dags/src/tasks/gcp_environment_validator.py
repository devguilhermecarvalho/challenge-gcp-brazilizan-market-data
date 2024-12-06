from airflow.decorators import task

@task
def task_cloud_storage_validator():
    from src.validators.cloud_storage_validator import CloudStorageValidator
    config_loader = "dags/src/configs/google_cloud.yml"
    cloud_storage_validator = CloudStorageValidator(config_path=config_loader)
    cloud_storage_validator.validate_or_create_buckets_and_tags()

@task
def task_bigquery_validator():
    from src.validators.bigquery_validator import BigQueryManager
    config_loader = "dags/src/configs/google_cloud.yml"
    bigquery_manager = BigQueryManager(config_path=config_loader)
    bigquery_manager.setup_datasets()
    