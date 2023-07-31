from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta  

try: 
    _config = Variable.get('config', deserialize_json=True)
    GOOGLE_CONN_ID  = _config.get('gcp_conn_id')                       
    PROJECT_ID      = _config.get('project_id') 
    # etc                
except:
    GOOGLE_CONN_ID = "google_cloud_default"
    PROJECT_ID= "byteflow-dev"

default_dag_args = {
   'owner': 'airflow',
   'start_date': datetime(2023,7,1),
   'retries': 0,
   'retry_delay': timedelta(minutes=10),
   'email': "tje.pionka@gmail.com",
   'email_on_failure': True
}

dag = DAG(
    dag_id="d_01_ingest_travel_data",
    schedule_interval= None,
    catchup = False,
    default_args=default_dag_args
)

task_01 = BigQueryInsertJobOperator(
    task_id='task_01_create_external_table',
    gcp_conn_id=GOOGLE_CONN_ID,
    configuration={
        "query": {
            "query": f'CALL `byteflow-dev.dataproc_dev.r_travels_01`();',
            "useLegacySql": False
        }
    },
    dag=dag
)

task_02 = BigQueryInsertJobOperator(
    task_id='task_02_create_internal_table',
    gcp_conn_id=GOOGLE_CONN_ID,
    configuration={
        "query": {
            "query": f'CALL `byteflow-dev.dataproc_dev.r_travels_02`();',
            "useLegacySql": False
        }
    },
    dag=dag
)

task_01 >> task_02
