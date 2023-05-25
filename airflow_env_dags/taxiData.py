from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from get_data import getData

default_args = {
    'owner': 'pablo',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'TaxiDataDAG',
    default_args = default_args,
    description = 'Green Taxi Trip Data',
    schedule = '@daily'
)

task_fileDownloader = PythonOperator(
    task_id = 'downloadFile',
    python_callable = getData,
    dag = dag
)

task_copyFile = BashOperator(
    task_id = 'copyFileFromLocal',
    bash_command = 'hadoop fs -copyFromLocal /home/pablo/data/taxi_data /user/pablo/data',
    dag = dag
)

task_extractData = HiveOperator(
    task_id = 'extractParquetFile',
    hql = open('/home/pablo/end_to_end_process/script.sql', 'r').read(),
    hive_cli_conn_id = 'hiveserver2_default',
    dag = dag
)

task_transformData = BashOperator(
    task_id = 'dataTransformation',
    bash_command = 'python3 /home/pablo/end_to_end_process/spark2.py',
    dag = dag
)

task_loadDataToDWH = HiveOperator(
    task_id = 'loadDataToDWH',
    hql = open('/home/pablo/end_to_end_process/script2.sql', 'r').read(),
    hive_cli_conn_id = 'hiveserver2_default',
    dag = dag
)

task_fileDownloader >> task_copyFile >> task_extractData >> task_transformData >> task_loadDataToDWH