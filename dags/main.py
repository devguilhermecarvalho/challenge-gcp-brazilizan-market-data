from pendulum import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig

from requests

# Parameters of the DAG
@dag(
    start_date=datetime(2024, 11, 11),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["example"]
)

def pipeline_kaggle_extractor():
    pass