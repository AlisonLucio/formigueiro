from airflow.models import Variable
from datetime import datetime, timedelta
import os

PROJECT_ID="video-aulas-ed"

CALL_API_GOV = {
    "DAG_CONFIG":{
        "DAG_ID":"apis_gov_br_dataproc",
        "PROJECT_ID":PROJECT_ID,
        "DEFAULT_ARGS":{
            'owner':'Alison',
            'start_date':datetime(2023, 6, 10),
            'retries': 4,
            'retry_delay': timedelta(seconds=120),
            'wait_for_downstream': False,
            'depends_on_past': True, # mudar isso quando tudo estiver rodando ok
        },
        'SCHEDULE_INTERVAL':'@daily',
        'CATCHUP':True, # mudar isso quando tudo estiver rodando ok
        'TAGS':['api_gov', 'cloud-function', 'dataproc', 'bigquery']
    },
}