from airflow.models import Variable
import requests

def on_failure_callback_to_teams(context):
    ti = context.get('ti')
    dag_id = ti.dag_id
    task_id = ti.task_id
    error_msg = context.get('exception')
    batch_date = context.get('data_interval_end').in_timezone('Asia/Seoul')

    webhook = Variable.get("msteams_webhook")
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": "Larry Bryant created a new task",
        "sections": [{
            "activityTitle": "airflow 실행 실패",
            "activitySubtitle": f"{dag_id}.{task_id}",
            "activityImage": "https://adaptivecards.io/content/cats/3.png",
            "facts": [{
                "name": "소유자",
                "value": "김성언"
            }, {
                "name": "실행 시간",
                "value": f"{batch_date}"
            }, {
                "name": "에러 내용",
                "value": f"{error_msg}"
            }],
            "markdown": True
        }],
    }
    headers = {"Content-Type": "application/json"}
    requests.post(webhook, json=payload, headers=headers)