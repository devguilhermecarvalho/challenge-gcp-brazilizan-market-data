class ServiceFactory:
    """Factory class to create instances of services like validators and extractors."""

    @staticmethod
    def create_bigquery_manager(config_path="configs/google_cloud.yml"):
        from src.validators.bigquery_validator import BigQueryManager
        return BigQueryManager(config_path)

    @staticmethod
    def create_cloud_storage_validator(config_path="configs/google_cloud.yml"):
        from src.validators.cloud_storage_validator import CloudStorageValidator
        return CloudStorageValidator(config_path)

    @staticmethod
    def create_kaggle_downloader(config_path="configs/kaggle.yml", kaggle_json_path='kaggle.json'):
        from src.endpoints.kaggle_extractor import KaggleDatasetDownloader
        from src.validators.validators_config_loader import ConfigLoader
        config_loader = ConfigLoader(config_path)
        kaggle_configs = config_loader.get_kaggle_configs()
        return KaggleDatasetDownloader(kaggle_json_path, config=kaggle_configs)