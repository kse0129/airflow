from airflow import DAG
from airflow.decorators import task

import pendulum
import datetime

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'success'
    
    @task(task_id='python_xcom_pull1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print(f'return value by xcom_pull directly: {value1}')

    @task(task_id='python_xcom_pull2')
    def xcom_pull_2(status, **kwargs):
        print(f"return value by function parameter: {status}")

    task_xcom_push = xcom_push_result()
    xcom_pull_2(task_xcom_push)
    task_xcom_push >> xcom_pull_1()