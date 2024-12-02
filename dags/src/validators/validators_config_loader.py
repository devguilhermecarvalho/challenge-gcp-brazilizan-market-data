import yaml

class ConfigLoader:
    def __init__(self, config_path="dags/src/configs/google_cloud.yml"):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        """Load the YAML configuration file."""
        with open(self.config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def get_default_parameters(self):
        """Return default parameters."""
        return self.config.get("default_parameters", {})

    def get_gcp_configs(self):
        """Return GCP-specific configurations."""
        return self.config.get("gcp_configs", {})

    def get_bucket_configs(self):
        """Return bucket configurations."""
        gcp_configs = self.get_gcp_configs()
        return gcp_configs.get("cloud_storage", {}).get("buckets", [])

    def get_kaggle_configs(self):
        """Return Kaggle-specific configurations."""
        return self.config.get("kaggle", {})