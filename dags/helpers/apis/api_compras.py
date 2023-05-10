from datetime import datetime, timedelta

PROJECT_ID="video-aulas-ed"

PATH_FILE = f'layer=incoming/source_type=api/file_type=csv/extraction=muda_extraction/\
    year={{{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y")}}}}/\
    mounth={{{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%m")}}}}/\
    day={{{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%d")}}}}/\
    hour={{{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%H")}}}}/\
    minute={{{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%M")}}}}/\
    mudar.csv'

call_api_gov = {
    "DAG_CONFIG":{
        "DAG_ID":"apis_gov_br",
        "PROJECT_ID":PROJECT_ID,
        "DEFAULT_ARGS":{
            'owner':'Alison',
            'start_date':datetime(2023, 4, 23),
            'retries': 3,
            'retry_delay': timedelta(minutes=1)
        },
        'SCHEDULE_INTERVAL':'@daily',
        'CATCHUP':False
    },
    "TASK_CONFIG":{
        "ZONE":"us-central1",
        'TASK_LIST':[
            {
                'task_id':'api_servicos_orgaos',
                'method':'GET',
                'http_conn_id':'http_conn_id_data_gov_br',
                'endpoint':'formiga-cortadeira',
                'data':{
                    'URL_API':'http://compras.dados.gov.br/servicos/v1/servicos.csv',
                    'PROJECT_ID':PROJECT_ID,
                    'BUCKET_NAME':PROJECT_ID,
                    'PATH_FILE':PATH_FILE.replace("mudar","api_servicos_orgaos")
                    .replace("muda_extraction","api_servicos_orgaos")
                },
                'headers':{"Content-Type": "application/json"}
            },
            {
                'task_id':'api_orgaos',
                'method':'GET',
                'http_conn_id':'http_conn_id_data_gov_br',
                'endpoint':'formiga-cortadeira',
                'data':{
                    'URL_API':'http://compras.dados.gov.br/licitacoes/v1/orgaos.csv',
                    'PROJECT_ID':PROJECT_ID,
                    'BUCKET_NAME':PROJECT_ID,
                    'PATH_FILE':PATH_FILE.replace("mudar","api_orgaos")
                    .replace("muda_extraction","api_orgaos")
                },
                'headers':{"Content-Type": "application/json"}
            },
            {
                'task_id':'api_fornecedores',
                'method':'GET',
                'http_conn_id':'http_conn_id_data_gov_br',
                'endpoint':'formiga-cortadeira',
                'data':{
                    'URL_API':'http://compras.dados.gov.br/fornecedores/v1/fornecedores.csv',
                    'PROJECT_ID':PROJECT_ID,
                    'BUCKET_NAME':PROJECT_ID,
                    'PATH_FILE':PATH_FILE.replace("mudar","api_fornecedores")
                    .replace("muda_extraction","api_fornecedores")
                },
                'headers':{"Content-Type": "application/json"}
            },
        ]
    } 
}