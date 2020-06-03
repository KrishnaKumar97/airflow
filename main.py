from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Initialize the default arguments
default_args = {
    'owner': 'Krishna',
    'depends_on_past': False,
    'start_date': datetime(year=2020, month=6, day=2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creating DAG
dag = DAG('corona_dag', default_args=default_args, description='A simple DAG for monitoring daily Corona cases',
          schedule_interval='@daily')

# task 1
t1 = BashOperator(task_id='perform_task_1', bash_command='python /home/nineleaps/PycharmProjects/airflow/task1.py',
                  dag=dag)

# task 2
t2 = BashOperator(
    task_id='perform_task_2',
    depends_on_past=True,
    bash_command='python /home/nineleaps/PycharmProjects/airflow/task2.py',
    retries=3,
    dag=dag,
)

# Defining dependency
t1 >> t2
