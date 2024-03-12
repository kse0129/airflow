from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow import DAG

import pendulum
import datetime

with DAG(
    dag_id="dags_xcom_bash2python",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'good', 'data':[1, 2, 3], 'options_cnt': 100}
        return result_dict
    
    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'STATUS':'{{ ti.xcom_pull(task_id="python_push").get("status") }}',
            'DATA':'{{ ti.xcom_pull(task_id="python_push").get("data") }}',
            'OPTIONS_CNT':'{{ ti.xcom_pull(task_id="python_push").get("options_cnt") }}',
        },
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )

    python_push_xcom() >> bash_pull
