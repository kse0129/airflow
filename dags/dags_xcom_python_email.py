from airflow.operators.email import EmailOperator
from airflow.decorators import task
from airflow import DAG

import pendulum


with DAG(
    dag_id="dags_xcom_python_email",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='something_task')
    def some_logic():
        from random import choice
        return choice(['success', 'fail'])
    
    send_email = EmailOperator(
        task_id='send_email',
        to='seongeon1999@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리 결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
                      {{ ti.xcom_pull(task_ids="something_task") }} 입니다'
    )