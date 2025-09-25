from __future__ import annotations
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from kafka import KafkaProducer
import json

@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description='DAG pour traiter les données Lichess avec Spark',
    tags=['lichess', 'spark'],
)
def lichess_streaming_dag():
    spark_submit = SparkSubmitOperator(
        task_id='spark_streaming_task',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/spark_streaming.py',  # chemin monté
        executor_memory='2g',
        executor_cores=2,
        driver_memory='2g',
        name='lichess_streaming_task',
    )

    spark_submit


dag_instance = lichess_streaming_dag()
