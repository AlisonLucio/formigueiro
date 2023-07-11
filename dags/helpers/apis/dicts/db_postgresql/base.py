from datetime import datetime, timedelta

PROJECT_ID="video-aulas-ed"

DAG_CONFIG={
    "DAG_ID":"postgresql_combustiveis_br",
    "PROJECT_ID":PROJECT_ID,
    "DEFAULT_ARGS":{
        'owner':'Alison',
        'start_date':datetime(2023, 6, 1),
        'end_date':datetime(2023, 6, 20),
        'retries': 4,
        'retry_delay': timedelta(seconds=120),
        'wait_for_downstream': False,
        'depends_on_past': True, # mudar isso quando tudo estiver rodando ok
    },
    'SCHEDULE_INTERVAL':'00 08 1-31 1-12 *',
    'CATCHUP':True, # mudar isso quando tudo estiver rodando ok
    'TAGS':['bigquery']
}