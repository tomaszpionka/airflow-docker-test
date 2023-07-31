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

date_from = datetime(2020, 1, 1)
date_to = datetime.now().strftime('%Y-%m-%d %H:%M:00')

default_dag_args = {
   'owner': 'airflow',
   'start_date': datetime(2023, 7, 31, 11, 0, 0),
   'retries': 0,
   'retry_delay': timedelta(minutes=5),
   'email': "tje.pionka@gmail.com",
   'email_on_failure': True
}

dag = DAG(
    dag_id="d_02_merge_travel_data",
    schedule_interval= None,
    catchup = False,
    default_args=default_dag_args,
    schedule = '*/20 * * * *'
)

task_01 = BigQueryInsertJobOperator(
    task_id='task_01_load_internal_table_with_range_of_external_table_data',
    gcp_conn_id=GOOGLE_CONN_ID,
    configuration={
        "query": {
            "query": f'CALL `byteflow-dev.dataproc_dev.r_travels_05`("{date_from}", "{date_to}");',
            "useLegacySql": False
        }
    },
    dag=dag
)

task_01

if __name__ == "__main__":
    print(date_from, date_to)