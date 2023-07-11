from datetime import datetime, timedelta

PROJECT_ID="video-aulas-ed"

CALL_API_GOV = {
    "DAG_CONFIG":{
        "DAG_ID":"apis_gov_br_batch_dataproc",
        "PROJECT_ID":PROJECT_ID,
        "DEFAULT_ARGS":{
            'owner':'Alison',
            'start_date':datetime(2023, 7, 6),
            'end_date':datetime(2023, 7, 12),
            'retries': 4,
            'retry_delay': timedelta(seconds=120),
            'wait_for_downstream': True,
            'depends_on_past': True, # mudar isso quando tudo estiver rodando ok
        },
        'SCHEDULE_INTERVAL':'* 5 1-31 1-12 *',
        'CATCHUP':True, # mudar isso quando tudo estiver rodando ok
        'TAGS':['api_gov', 'cloud-function', 'dataproc', 'bigquery']
    },
}