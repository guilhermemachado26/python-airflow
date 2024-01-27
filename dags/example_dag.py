import logging
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

logger = logging.getLogger(__name__)

### Tasks ###
def task_get_random_number():
    response = requests.get('https://www.randomnumberapi.com/api/v1.0/random?min=0&max=100&count=1')
    return response.json()[0]

def task_is_greater_than_50(task_instance):
    number = task_instance.xcom_pull(task_ids='get_random_number')

    return 'is_greater' if number > 50 else 'is_not_greater'


with DAG('dag_tutorial', start_date=datetime(2021, 12, 1), schedule_interval='30 * * * *', catchup=False) as dag:
    get_random_number = PythonOperator(
        task_id='get_random_number',
        python_callable=task_get_random_number,
    )

    is_greater_than_50 = BranchPythonOperator(
        task_id='is_greater_than_50',
        python_callable=task_is_greater_than_50,
    )

    is_greater = BashOperator(
        task_id='is_greater',
        bash_command='echo "is greater"'
    )

    is_not_greater = BashOperator(
        task_id='is_not_greater',
        bash_command='echo "is not greater"'
    )

    
get_random_number >> is_greater_than_50 >> [is_greater, is_not_greater]