from airflow import DAG
from airflow.operators.bash import BashOperator
from config.on_failure_callback_to_teams import on_failure_callback_to_teams
import pendulum
from datetime import timedelta

with DAG(
    dag_id='failure_teams_test',
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
    default_args={
        'on_failure_callback': on_failure_callback_to_teams,
        'execution_timeout': timedelta(seconds=60)
    }
) as dag:

    task_sleep_90 = BashOperator(
        task_id='task_sleep_90',
        bash_command='sleep 90'
    )

    task_exit_1 = BashOperator(
        task_id='task_exit_1',
        bash_command='exit 1'
    )

    task_sleep_90 >> task_exit_1