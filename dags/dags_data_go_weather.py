from operators.data_go_kr_operator import DataGoKrCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='data_go_weather',
    schedule='* * * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    short_weather = DataGoKrCsvOperator(
        task_id='short_weather',
        path='/opt/airflow/files/short_weather/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='short_weather.csv',
        provider='13600000',
        api_name=['VilageFcstInfoService_2.0', 'getUltraSrtNcst'],
        numOfRows="10",
        pageNo="1",
        dataType="JSON",
        base_date="20240314",
        base_time="0600",
        nx='55',
        ny='127'
    )

    short_weather