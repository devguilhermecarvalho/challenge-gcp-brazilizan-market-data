from pendulum import datetime
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from src.tasks.gcp_environment_validator import task_cloud_storage_validator, task_bigquery_validator
from src.tasks.kaggle_extractor_tasks import (
    get_kaggle_data_validator_task,
    extract_kaggle_datasets,
    kaggle_data_uploader
)
from src.tasks.dbt_transform_task import get_dbt_transform_taskgroup

@dag(
    start_date=datetime(2024, 11, 11),
    schedule="@daily",
    catchup=False,
)
def pipeline_kaggle_extractor():
    """
    Task Group: Validação do Ambiente do Cloud Storage e BigQuery
    """
    with TaskGroup(group_id='gcp_environment_validator') as gcp_environment_validator_task_group:
        # Chamar as tarefas importadas
        task1 = task_bigquery_validator()
        task2 = task_cloud_storage_validator()

    """
    Obter a lista de dataset_ids antes de definir as tasks
    """
    from src.validators.validators_config_loader import ConfigLoader

    config_loader = ConfigLoader(config_path="dags/src/configs/kaggle.yml")
    kaggle_configs = config_loader.get_kaggle_configs()
    dataset_ids = [dataset['id'] for dataset in kaggle_configs.get('datasets', [])]

    """
    Task Group: Extração e Validação do Kaggle
    """
    with TaskGroup(group_id='kaggle_extractor') as kaggle_extractor_task_group:
        kaggle_data_validator = get_kaggle_data_validator_task(dataset_ids)

        extract_tasks = extract_kaggle_datasets.expand(dataset_id=dataset_ids)
        uploader_tasks = kaggle_data_uploader.expand(extracted_data_info=extract_tasks)

        kaggle_task_group_complete = EmptyOperator(task_id='kaggle_task_group_complete')

        # Definir dependências
        kaggle_data_validator >> [extract_tasks, kaggle_task_group_complete]
        extract_tasks >> uploader_tasks >> kaggle_task_group_complete

    """
    Task Group: DBT
    """
    dbt_transform = get_dbt_transform_taskgroup()

    # Estabelecer as dependências entre os task groups
    gcp_environment_validator_task_group >> kaggle_extractor_task_group >> dbt_transform

pipeline_kaggle_extractor()
