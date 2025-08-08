from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="flink_ratings_job",
    start_date=pendulum.datetime(2025, 7, 10, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    tags=["flink", "streaming"],
    doc_md="""
    ### Flink Ratings Job
    Submits a sample PyFlink streaming job to the Flink cluster.
    """,
) as dag:

    submit_flink_job = BashOperator(
        task_id="submit_flink_job",
        bash_command="flink run -py /opt/flink/usrlib/ratings_stream.py",
    )
