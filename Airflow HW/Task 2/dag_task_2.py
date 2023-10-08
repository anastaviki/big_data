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
    'owner': 'HW_Task_2',
    'start_date': datetime(2023, 10, 8),
    'retries': 1,
}
dag = DAG(
    "HW_Task_2",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False, 
)


def download_csv_file():
    url = "https://www.stats.govt.nz/assets/Uploads/Greenhouse-gas-emissions-by-region-industry-and-household/Greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018/Download-data/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv"
    response = requests.get(url)
    with open("/tmp/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018.csv", "wb") as f:
        f.write(response.content)

download_task = PythonOperator(
    task_id="download_csv_file",
    python_callable=download_csv_file,
    dag=dag,
)

def count_rows():
    df = pd.read_csv("/tmp/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018.csv")
    row_count = len(df)
    return row_count

count_rows_task = PythonOperator(
    task_id="count_rows",
    python_callable=count_rows,
    dag=dag,
)

def convert_to_avro():
    df = pd.read_csv("/tmp/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018.csv")
    

    avro_schema = avro.schema.make_avsc_object({
        "type": "record",
        "name": "GreenhouseGasData",
        "fields": [
            {"name": "region", "type": ["string", "null"]},
            {"name": "anzsic_descriptor", "type": ["string", "null"]},
            {"name": "gas", "type": ["string", "null"]},
            {"name": "units", "type": ["string", "null"]},
            {"name": "magnitude", "type": ["string", "null"]},
            {"name": "year", "type": ["int", "null"]},
            {"name": "data_val", "type": ["double", "null"]}
        ]
    })
    
    try:
        with open("/tmp/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018.avro", "wb") as f:
            writer = DataFileWriter(f, DatumWriter(), avro_schema)
            for _, row in df.iterrows():
                try:
                    writer.append(row.to_dict())
                except Exception as inner_exception:
                    print(f"An error occurred during an iteration: {str(inner_exception)}")
            writer.close()
        print("Avro file successfully created.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

convert_to_avro_task = PythonOperator(
    task_id="convert_to_avro",
    python_callable=convert_to_avro,
    dag=dag,
)


download_task >> count_rows_task >> convert_to_avro_task
