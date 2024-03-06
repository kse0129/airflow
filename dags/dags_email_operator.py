from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    send_mail_task = EmailOperator(
        task_id = "send_mail_task",
        to="seongeon1999@naver.com",
        subject="[Airflow success]",
        html_content="<h1>Airflow 메일 전송 성공"
    )