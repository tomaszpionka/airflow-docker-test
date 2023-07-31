from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta                                                  
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)
from airflow.providers.google.cloud.operators import bigquery


# refer to admin panel configuration as key-value stored - easy for dev/qa/prod environments management

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="byteflow-dev"
CLUSTER_NAME = 'cluster-dataproc-dev'
REGION = 'us-central1'
PYSPARK_URI = f'gs://byteflow_dataproc_dev/dependencies/read_csv.py'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,

    },
}
        # "args": [{"--dataset":"dataproc_dev"}]

s = "2018-01-01"
load_start_date = datetime.strptime(s, "%Y-%m-%d")
load_start_date = datetime.strftime(load_start_date, "%Y-%m-%d")

default_dag_args = {
   'owner': 'airflow',
   'start_date': datetime(2023,5,18),
   'retries': 0,
   'retry_delay': timedelta(minutes=10),
   'email': "tje.pionka@gmail.com",
   'email_on_failure': True
}

def python_fun():
    print("hello there")

dag = DAG(
   dag_id="_dag_test",
   schedule_interval= None,
   catchup = False,
   default_args=default_dag_args
)

python_task = PythonOperator(
    task_id='python_task_1',
    python_callable=python_fun,
    dag=dag
)

task_custom = bigquery.BigQueryInsertJobOperator(
    task_id='task_custom_connection',
    gcp_conn_id=GOOGLE_CONN_ID,
    configuration={
        "query": {
            "query": 'SELECT 1',
            "useLegacySql": False
        }
    },
    dag=dag
)

pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task_1", 
    job=PYSPARK_JOB, 
    region=REGION, 
    project_id=PROJECT_ID,
    gcp_conn_id=GOOGLE_CONN_ID,
    dag=dag
)

python_task >> task_custom >> pyspark_task