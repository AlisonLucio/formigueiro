from datetime import datetime, timedelta
from helpers.apis.querys.api import incoming_to_raw


def changePathFile(change_file_type:str, change_table_name:str):
    return f'layer_incoming/source_type_api/file_type_{change_file_type}/{change_table_name}/'\
            'year={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y")}}/'\
            'mounth={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%m")}}/'\
            'day={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%d")}}/'\
            'hour={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%H")}}/'\
            'minute={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%M")}}/'\
            'prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}/'\
            f'{change_table_name}.csv'

PROJECT_ID="video-aulas-ed"



TABLE_NAME_LIST = [
    'api_servicos_orgaos',
    'api_orgaos',
    'api_fornecedores'
]

CALL_API_GOV = {
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
        "PIPELINES_TABLES":{
            "api_servicos_orgaos":{
                "CLOUD_FUNCTION":{
                    'task_id':'extration',
                    'method':'GET',
                    'http_conn_id':'http_conn_id_data_gov_br',
                    'endpoint':'formiga-cortadeira',
                    'data':{
                        'URL_API':'http://compras.dados.gov.br/servicos/v1/servicos.csv',
                        'PROJECT_ID':PROJECT_ID,
                        'BUCKET_NAME':PROJECT_ID,
                        'PATH_FILE':changePathFile("csv", "api_servicos_orgaos")
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                "BIGQUERY":{
                    'task_id':'bq_incoming_to_raw',
                    'location':{
                        "query":{
                            "query":incoming_to_raw('api_servicos_orgaos'),
                            "useLegacySql":False
                        }
                    },
                    'configuration':'Location',
                    'deferrable':True
                }
            },
            'api_orgaos':{
                "CLOUD_FUNCTION":{
                    'task_id':'extration',
                    'method':'GET',
                    'http_conn_id':'http_conn_id_data_gov_br',
                    'endpoint':'formiga-cortadeira',
                    'data':{
                        'URL_API':'http://compras.dados.gov.br/licitacoes/v1/orgaos.csv',
                        'PROJECT_ID':PROJECT_ID,
                        'BUCKET_NAME':PROJECT_ID,
                        'PATH_FILE':changePathFile("csv", "api_orgaos")
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                # "BIGQUERY":{
                #     'task_id':'bq_incoming_to_raw',
                #     'location':{
                #         "query":{
                #             "query":api(''),
                #             "useLegacySql":False
                #         }
                #     },
                #     'configuration':Location,
                #     'deferrable':True
                # }
            },
            'api_fornecedores':{
                'CLOUD_FUNCTION':{
                    'task_id':'extration',
                    'method':'GET',
                    'http_conn_id':'http_conn_id_data_gov_br',
                    'endpoint':'formiga-cortadeira',
                    'data':{
                        'URL_API':'http://compras.dados.gov.br/fornecedores/v1/fornecedores.csv',
                        'PROJECT_ID':PROJECT_ID,
                        'BUCKET_NAME':PROJECT_ID,
                        'PATH_FILE':changePathFile("csv", "api_fornecedores")
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                # "BIGQUERY":{
                #     'task_id':'incoming_to_raw',
                #     'location':{
                #         "query":{
                #             "query":file_path,
                #             "useLegacySql":False
                #         }
                #     },
                #     'configuration':Location,
                #     'deferrable':True
                # }
            }
        }    
    }
}