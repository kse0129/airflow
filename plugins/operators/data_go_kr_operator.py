from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class DataGoKrCsvOperator(BaseOperator):
    template_fields = ('path', 'file_name')

    def __init__(self, path, file_name, provider, api_name, params, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'data.go.kr'
        self.path = path
        self.file_name = file_name
        self.provider = provider
        self.api_name = '/'.join(api_name)
        self.params = params

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}/{self.provider}/{self.api_name}'

        total_row_df = self._call_api(self.base_url, **self.params)

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, **kwargs):
        import requests
        import json

        response = requests.get(base_url, params=kwargs)
        contents = json.loads(response.text)['response']['body']['items']['item']
        row_df = pd.DataFrame(contents)

        return row_df