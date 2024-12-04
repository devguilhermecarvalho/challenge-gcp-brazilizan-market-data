from pathlib import Path
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from airflow.models import Connection
from airflow import settings
import os
import json

def create_gcp_connection():
    conn_id = 'google_cloud_default'  # Substitua pelo seu conn_id desejado

    # Verificar se a conexão já existe
    session = settings.Session()
    conn_exists = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if not conn_exists:
        # Obter o caminho para o arquivo de chave da variável de ambiente
        keyfile_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if not keyfile_path:
            raise ValueError("A variável de ambiente GOOGLE_APPLICATION_CREDENTIALS não está definida.")

        # Carregar o conteúdo do arquivo de chave
        with open(keyfile_path, 'r') as f:
            keyfile_dict = json.load(f)

        conn = Connection(
            conn_id=conn_id,
            conn_type='google_cloud_platform',
            extra=json.dumps({
                'extra__google_cloud_platform__keyfile_dict': keyfile_dict,
                'extra__google_cloud_platform__scope': 'https://www.googleapis.com/auth/cloud-platform',
            })
        )
        session.add(conn)
        session.commit()
    session.close()

def get_dbt_transform_taskgroup():
    create_gcp_connection()

    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
            conn_id="google_cloud_default",  # Deve corresponder ao conn_id usado em create_gcp_connection
            profile_args={
                "schema": "your_bigquery_dataset_name",  # Substitua pelo nome do seu dataset no BigQuery
                "project": "your_gcp_project_id",       # Substitua pelo ID do seu projeto GCP
            },
        ),
    )

    # Ajustar o caminho para o dbt_project.yml
    dbt_project_path = Path('/usr/local/airflow/dags/dbt_transformations')

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(dbt_project_path=dbt_project_path),
        profile_config=profile_config,
        default_args={"owner": "Astro", "retries": 1},
        operator_args={
            "dbt_executable_path": "/usr/local/airflow/.local/bin/dbt"
        }
    )
    return dbt_transform
