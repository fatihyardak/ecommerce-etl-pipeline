from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.triggers.base import StartTriggerArgs
import pendulum


with DAG(
    dag_id = "ecommerce_etl_pipeline",
    start_date = pendulum.datetime(2025, 8, 13, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False, 

    tags=["ecommerce", "spark", "gcp"]
) as dag: 

start_task = EmptyOperator(task_id="start")

end_task = EmptyOperator(task_id="end")

start_task >> end_task