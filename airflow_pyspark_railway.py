from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, set schedule_interval to None for manual trigger
dag = DAG(
    dag_id='pyspark_airflow_railway_etl',
    default_args=default_args,
    description='Manual trigger for PySpark ETL with SparkSubmitOperator',
    schedule_interval=None,  # No automatic scheduling, only manual
    catchup=False,           # Don't run missed DAG runs
)

# Define the SparkSubmitOperator
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_job',
    application='/home/cipher/pyspark_files/pyspark_airflow_railway.py',
    conn_id='spark_default',
    executor_cores=2,
    executor_memory='2g',
    jars='/home/cipher/pyspark_files/postgresql-42.7.3.jar',
    total_executor_cores=2,
    name='CSV_ETL_To_PostgreSQL_From_Railway',
    verbose=True,
    conf={'spark.master': 'local[*]'},  # Set the master to local mode with all cores
    dag=dag
)