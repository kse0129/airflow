from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import requests
import json

with DAG(
    dag_id='teams_test',
    default_args={
        'owner': '김성언',
    },
    start_date=datetime(2024, 4, 1),
    schedule=None,
) as dag:

    @task
    def send_message_to_ms_teams():
        webhook = Variable('msteams_webhook')
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