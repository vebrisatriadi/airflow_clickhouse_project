# dags/dag_01_kaggle_downloader.py

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id='kaggle_downloader',
    start_date=pendulum.datetime(2025, 7, 10, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    tags=['data_source', 'kaggle'],
    doc_md="""
    ### Kaggle Downloader DAG
    This DAG downloads the TMDB dataset from Kaggle and unzips it into the data folder.
    **Prerequisites**:
    - Place your `kaggle.json` in the project root.
    - The docker-compose must mount `kaggle.json` to `/home/airflow/.kaggle/kaggle.json`.
    """
) as dag:
    
    set_kaggle_api_permissions = BashOperator(
        task_id='set_kaggle_api_permissions',
        bash_command='chmod 600 /home/airflow/.kaggle/kaggle.json',
    )

    download_and_unzip_dataset = BashOperator(
        task_id='download_and_unzip_dataset',
        bash_command='kaggle datasets download edgartanaka1/tmdb-movies-and-series -p /opt/airflow/data --unzip',
    )

    set_kaggle_api_permissions >> download_and_unzip_dataset