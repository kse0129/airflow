from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import timedelta
import pendulum

email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul')
) as dag:
    pass