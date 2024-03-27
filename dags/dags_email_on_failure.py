from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta

email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_email_on_failure',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule='0 1 * * *',
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True,
        'email': email_list
    }
) as dags:
    @task(task_id='python_fail')
    def python_fail():
        raise AirflowException('에러 발생')
    python_fail()

    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1'
    )

    bash_success = BashOperator(
        task_id='bash_success',
        bash_command='exit 0'
    )