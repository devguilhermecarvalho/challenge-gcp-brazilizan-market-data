from google.cloud import storage
from dags.src.validators.validators_config_loader import ConfigLoader
import os

class CloudStorageValidator:
    def __init__(self, config_path="google_cloud.yml"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_cloud.json"

        self.config_loader = ConfigLoader(config_path)
        self.client = storage.Client()
        self.default_parameters = self.config_loader.get_default_parameters()
        self.buckets_config = self.config_loader.get_bucket_configs()

    def validate_or_create_buckets_and_tags(self):
        """Valida, cria e aplica configurações de buckets e tags."""
        for bucket_config in self.buckets_config:
            bucket_name = bucket_config["name"]
            bucket_options = bucket_config.get("options", {})
            folders = bucket_config.get("folders", [])
            bucket_tags = self._merge_tags(bucket_options.get("tags", {}), self.default_parameters.get("tags", {}))

            bucket = self._get_or_create_bucket(bucket_name, bucket_options)
            self._validate_and_apply_tags(bucket, bucket_tags)
            self._create_folders(bucket, folders)

    def _get_or_create_bucket(self, bucket_name, bucket_options):
        """Obtém o bucket se ele existir ou cria um novo com as configurações fornecidas."""
        try:
            bucket = self.client.get_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' encontrado.")
        except Exception:
            print(f"Bucket '{bucket_name}' não encontrado. Criando...")
            bucket = self.client.bucket(bucket_name)

            # Configuração do bucket
            bucket.location = bucket_options.get("region", self.default_parameters.get("region"))
            bucket.storage_class = bucket_options.get("storage_class", self.default_parameters.get("storage_class"))
            bucket.versioning_enabled = bucket_options.get("versioning", self.default_parameters.get("versioning"))

            # Criação do bucket
            bucket = self.client.create_bucket(bucket)
            print(f"Bucket '{bucket_name}' criado com sucesso.")

        return bucket

    def _merge_tags(self, bucket_tags, default_tags):
        """Mescla as tags do bucket com as tags padrão."""
        merged_tags = default_tags.copy()
        merged_tags.update(bucket_tags)
        return merged_tags

    def _validate_and_apply_tags(self, bucket, tags):
        """Valida e aplica as tags no bucket."""
        existing_tags = bucket.labels

        if existing_tags != tags:
            bucket.labels = tags
            bucket.patch()  # Aplica as mudanças no bucket
            print(f"Tags atualizadas no bucket '{bucket.name}': {tags}")
        else:
            print(f"As tags no bucket '{bucket.name}' já estão atualizadas.")

    def _create_folders(self, bucket, folders):
        """Cria folders simulados no bucket."""
        for folder_name in folders:
            blob = bucket.blob(f"{folder_name}/")  # Simula uma pasta
            if not blob.exists():
                blob.upload_from_string("")  # Cria um blob vazio como marcador
                print(f"Folder '{folder_name}' criado em '{bucket.name}'.")
            else:
                print(f"Folder '{folder_name}' já existe em '{bucket.name}'.")

# Uso do validador
validator = CloudStorageValidator()
validator.validate_or_create_buckets_and_tags()