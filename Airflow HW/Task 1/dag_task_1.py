from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'HW_Task_1',
    'start_date': datetime(2023, 9, 17),
    'retries': 1,
}

dag = DAG(
    'HW_Task_1',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,  
)


def print_good_morning():
    print("Good Morning")


def print_good_day():
    print("Good Day")


def print_good_evening():
    print("Good Evening")


task_morning = PythonOperator(
    task_id='print_morning',
    python_callable=print_good_morning,
    dag=dag,
)


task_day = PythonOperator(
    task_id='print_day',
    python_callable=print_good_day,
    dag=dag,
)


task_evening = PythonOperator(
    task_id='print_evening',
    python_callable=print_good_evening,
    dag=dag,
)

task_morning >> task_day >> task_evening
