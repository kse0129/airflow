from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import requests
import pendulum

with DAG(
    dag_id='teams_test',
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:

    @task(task_id="send_message_to_ms_teams")
    def send_message_to_ms_teams():
        webhook = Variable.get("msteams_webhook")
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": "Larry Bryant created a new task",
            "sections": [{
                "activityTitle": "airflow 테스트",
                "activitySubtitle": "airflow 전송 성공",
                "activityImage": "https://adaptivecards.io/content/cats/3.png",
                "facts": [{
                    "name": "소유자",
                    "value": "김성언"
                }, {
                    "name": "실행 시간",
                    "value": pendulum.now()
                }, {
                    "name": "상태",
                    "value": "전송 성공"
                }],
                "markdown": True
            }],
        }
        headers = {"Content-Type": "application/json"}
        requests.post(webhook, json=payload, headers=headers)
 
    send_message_to_ms_teams()