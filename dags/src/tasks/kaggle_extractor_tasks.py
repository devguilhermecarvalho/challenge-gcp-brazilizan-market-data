from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator

def kaggle_data_validation(dataset_ids, **kwargs):
    from src.validators.validators_config_loader import ConfigLoader
    from src.validators.kaggle_data_validator import KaggleDataValidator

    config_loader = ConfigLoader(config_path="dags/src/configs/kaggle.yml")
    kaggle_configs = config_loader.get_kaggle_configs()

    validator = KaggleDataValidator(config=kaggle_configs)
    datasets_exist = validator.validate_datasets(dataset_ids)

    # Decidir se deve prosseguir com a extração ou pular
    if all(datasets_exist.values()):
        return 'kaggle_extractor.kaggle_task_group_complete'
    else:
        return 'kaggle_extractor.extract_kaggle_datasets'

def get_kaggle_data_validator_task(dataset_ids):
    return BranchPythonOperator(
        task_id='kaggle_data_validator',
        python_callable=kaggle_data_validation,
        op_kwargs={'dataset_ids': dataset_ids},
        provide_context=True
    )

@task(task_id='extract_kaggle_datasets', multiple_outputs=True)
def extract_kaggle_datasets(dataset_id):
    from src.validators.validators_config_loader import ConfigLoader
    from src.extractors.kaggle_data_extractor import KaggleDataExtractor

    config_loader = ConfigLoader(config_path="dags/src/configs/kaggle.yml")
    kaggle_configs = config_loader.get_kaggle_configs()

    extractor = KaggleDataExtractor(config=kaggle_configs)

    result = extractor.extract_dataset(dataset_id)
    if result:
        file_path, dataset_name, update_date = result
        return {
            'file_path': file_path,
            'dataset_name': dataset_name,
            'update_date': update_date,
            'base_folder': os.path.dirname(file_path)
        }
    else:
        print(f"Falha ao extrair o dataset {dataset_id}")
        return None

@task(task_id='kaggle_data_uploader')
def kaggle_data_uploader(extracted_data_info):
    from src.validators.validators_config_loader import ConfigLoader
    from src.uploaders.kaggle_data_uploader import KaggleDataUploader

    if extracted_data_info is None:
        return

    config_loader = ConfigLoader(config_path="dags/src/configs/kaggle.yml")
    kaggle_configs = config_loader.get_kaggle_configs()

    uploader = KaggleDataUploader(config=kaggle_configs)

    file_path = extracted_data_info['file_path']
    dataset_name = extracted_data_info['dataset_name']
    update_date = extracted_data_info['update_date']
    base_folder = extracted_data_info['base_folder']
    uploader.upload_file_to_gcs(file_path, dataset_name, update_date)
    uploader.cleanup_local_files(base_folder)
