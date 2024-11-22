import yaml

class ConfigLoader:
    def __init__(self, config_path="google_cloud.yml"):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        """Carrega o arquivo de configuração YAML."""
        with open(self.config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def get_default_parameters(self):
        """Retorna os parâmetros padrão."""
        return self.config.get("default_parameters", {})

    def get_gcp_configs(self):
        """Retorna as configurações específicas do GCP."""
        return self.config.get("gcp_configs", {})

    def get_bucket_configs(self):
        """Retorna as configurações dos buckets."""
        gcp_configs = self.get_gcp_configs()
        return gcp_configs.get("cloud_storage", {}).get("buckets", [])