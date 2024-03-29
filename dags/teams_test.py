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
        webhook = Variable.get('msteams_webhook')
        print(1)
        payload = {
          "@type": "MessageCard",
          "@context": "http://schema.org/extensions",
          "summary": "Summary",
          "sections": [{
            "activityTitle": "에어플로우 테스트",
            "activitySubtitle": "전송 성공",
            "facts": [
              {
                "키1": "값1",
                "키2": "값2"
              }
            ],
            "text": "Text"
          }],
          "potentialAction": [{
            "@type": "OpenUri",
            "name": "Link name",
            "targets": [{
              "os": "default",
              "uri": "https://www.google.com/"
            }]
          }]
        }
        headers = {"content-type": "application/json"}
        requests.post(webhook, json=payload, headers=headers)
 
    send_message_to_ms_teams()