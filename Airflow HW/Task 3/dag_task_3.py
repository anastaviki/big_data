from airflow import DAG
from airflow.operators.python_operator import PythonOperator  
from airflow.utils.dates import days_ago
import pandas as pd
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import requests
from datetime import datetime

default_args = {
    'owner': 'HW_Task_3',
    'start_date': datetime(2023, 10, 8),
    'retries': 1,
}
dag = DAG(
    "HW_Task_3",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False, 
)


def download_csv_file():
    url = "https://www.stats.govt.nz/assets/Uploads/Greenhouse-gas-emissions-by-region-industry-and-household/Greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018/Download-data/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv"
    response = requests.get(url)
    with open("/tmp/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018.csv", "wb") as f:
        f.write(response.content)

download_csv = PythonOperator(
    task_id="download_csv_file",
    python_callable=download_csv_file,
    dag=dag,
)


def process_csv_and_create_pivot():

    df = pd.read_csv('/tmp/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018.csv')

    pivot_table = df.groupby(['region', 'year']).agg({'data_val': 'sum'}).reset_index()

    pivot_table.to_csv('/tmp/result.csv', index=False)

process_csv_task = PythonOperator(
    task_id='process_csv_and_create_pivot',
    python_callable=process_csv_and_create_pivot,
    dag=dag,
)


download_csv >> process_csv_task 
