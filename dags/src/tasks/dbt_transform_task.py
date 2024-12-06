from pathlib import Path
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from airflow.models import Connection
from airflow import settings
import os
import json
from contextlib import closing

class DBTBuilder:
    def __init__(self):
        self.connection_id = "google_cloud_default"
        self.connection_type = "google_cloud_platform"
        self.bigquery_dataset = os.getenv("BIGQUERY_DATASET")
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.path_dbt_project_yml_config = os.getenv(
            "DBT_PROJECT_PATH", "/usr/local/airflow/dags/dbt_transformations"
        )
        self.profile_name = "default"
        self.target_name = "dev"

    def create_gcp_connection(self) -> None:
        """Cria uma conexão GCP no Airflow, caso ainda não exista."""
        try:
            with closing(settings.Session()) as session:
                conn_exists = session.query(Connection).filter(Connection.conn_id == self.connection_id).first()

                if conn_exists:
                    print(f"Connection '{self.connection_id}' already exists.")
                    return

                keyfile_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
                if not keyfile_path:
                    raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS not set.")

                with open(keyfile_path, "r") as f:
                    keyfile_dict = json.load(f)

                conn = Connection(
                    conn_id=self.connection_id,
                    conn_type=self.connection_type,
                    extra=json.dumps({
                        "extra__google_cloud_platform__keyfile_dict": keyfile_dict,
                        "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
                    }),
                )
                session.add(conn)
                session.commit()
                print(f"Connection '{self.connection_id}' created successfully.")
        except Exception as e:
            print(f"Failed to create GCP connection: {e}")

    def get_dbt_transform_taskgroup(self) -> DbtTaskGroup:
        """Configura e retorna o TaskGroup para as transformações DBT."""
        self.create_gcp_connection()

        profile_config = ProfileConfig(
            profile_name=self.profile_name,
            target_name=self.target_name,
            profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
                conn_id=self.connection_id,
                profile_args={
                    "schema": self.bigquery_dataset,
                    "project": self.project_id,
                },
            ),
        )

        dbt_project_path = Path(self.path_dbt_project_yml_config)

        dbt_transform = DbtTaskGroup(
            group_id="dbt_transform",
            project_config=ProjectConfig(dbt_project_path=dbt_project_path),
            profile_config=profile_config,
            default_args={"owner": "Gui", "retries": 1},
            operator_args={
                "dbt_executable_path": "/usr/local/airflow/.local/bin/dbt"
            },
        )
        return dbt_transform