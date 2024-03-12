from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum

with DAG(
    dag_id="dags_trigger_dag_run_operator",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    schedule="30 9 * * *",
    catchup=False
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start"',
    )

    trigger_tag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        execution_date='{{ data_interval_start.in_timezone("Asia/Seoul") }}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
    )

    start_task >> trigger_tag_task