from datetime import datetime, timedelta
from airflow.models import Variable

PROJECT_ID="video-aulas-ed"
DATASET_ID_INCOMING="incoming"
DATASET_ID_RAW="raw"
DATASET_ID_TRUSTED="trusted"
LOCATION="us-central1"
SERVICE_ACCOUNT=Variable.get('SERVICE_ACCOUNT')
GCP_CONN_ID= 'google_cloud_default'


API_SERVICOS_ORGAOS='api_servicos_orgaos'
API_ORGAOS='api_orgaos'
API_FORNECEDORES='api_fornecedores'


TABLE_NAME_LIST = [
    API_SERVICOS_ORGAOS,
    API_ORGAOS,
    API_FORNECEDORES
]

def changePathFile(change_layer:str, change_file_type:str, change_table_name:str, change_file_extension:str):
    return f'layer_{change_layer}/source_type_api/file_type_{change_file_type}/{change_table_name}/'\
            'year={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y")}}/'\
            'mounth={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%m")}}/'\
            'day={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%d")}}/'\
            'hour={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%H")}}/'\
            'minute={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%M")}}/'\
            'prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}/'\
            f'{change_table_name}.{change_file_extension}'

CALL_API_GOV = {
    "DAG_CONFIG":{
        "DAG_ID":"apis_gov_br",
        "PROJECT_ID":PROJECT_ID,
        "DEFAULT_ARGS":{
            'owner':'Alison',
            'start_date':datetime(2023, 5, 1),
            'retries': 3,
            'retry_delay': timedelta(seconds=10),
            'depends_on_past': True
        },
        'SCHEDULE_INTERVAL':'@daily',
        'CATCHUP':True,
        'TAGS':['cloud-function', 'bigquery']
    },
    "TASK_CONFIG":{
        "ZONE":LOCATION,
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
                        'PATH_FILE':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv")
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                'DELETE_FILE':{
                    'INCOMING':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                    'RAW':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                    'TRUSTED':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                },
                'TRANSFER_STORAGE_DATA':{
                    'INCOMING_TO_RAW':{
                        'source_bucket':PROJECT_ID,
                        'source_object':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'destination_bucket':PROJECT_ID,
                        'destination_object':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'move_object':True
                    },
                     'RAW_TO_TRUSTED':{
                        'source_bucket':PROJECT_ID,
                        'source_object':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'destination_bucket':PROJECT_ID,
                        'destination_object':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'move_object':False
                    },
                    'TRUSTED_TO_BIGQUERY':{
                        'bucket':PROJECT_ID,
                        'source_objects':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv"),
                        'destination_project_dataset_table': f'trusted.{API_SERVICOS_ORGAOS}',
                        'write_disposition':'WRITE_APPEND',
                        'external_table':False,
                        'autodetect':True,
                        'max_id_key':None,
                        'deferrable':False
                    }                   
                },               
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
                        'PATH_FILE':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv")
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                'DELETE_FILE':{
                    'INCOMING':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                    'RAW':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                    'TRUSTED':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                },
                'TRANSFER_STORAGE_DATA':{
                    'INCOMING_TO_RAW':{
                        'source_bucket':PROJECT_ID,
                        'source_object':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'destination_bucket':PROJECT_ID,
                        'destination_object':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'move_object':True
                    },
                     'RAW_TO_TRUSTED':{
                        'source_bucket':PROJECT_ID,
                        'source_object':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'destination_bucket':PROJECT_ID,
                        'destination_object':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'move_object':False
                    },
                    'TRUSTED_TO_BIGQUERY':{
                        'bucket':PROJECT_ID,
                        'source_objects':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv"),
                        'destination_project_dataset_table': f'trusted.{API_ORGAOS}',
                        'write_disposition':'WRITE_APPEND',
                        'external_table':False,
                        'autodetect':True,
                        'max_id_key':None,
                        'deferrable':False
                    }                   
                },
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
                        'PATH_FILE':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv")
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                'DELETE_FILE':{
                    'INCOMING':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                    'RAW':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                    'TRUSTED':{
                        'bucket_name':PROJECT_ID,
                        'prefix':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                },
                'TRANSFER_STORAGE_DATA':{
                    'INCOMING_TO_RAW':{
                        'source_bucket':PROJECT_ID,
                        'source_object':changePathFile(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'destination_bucket':PROJECT_ID,
                        'destination_object':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'move_object':True
                    },
                     'RAW_TO_TRUSTED':{
                        'source_bucket':PROJECT_ID,
                        'source_object':changePathFile(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'destination_bucket':PROJECT_ID,
                        'destination_object':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'move_object':False
                    },
                    'TRUSTED_TO_BIGQUERY':{
                        'bucket':PROJECT_ID,
                        'source_objects':changePathFile(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv"),
                        'destination_project_dataset_table': f'trusted.{API_FORNECEDORES}',
                        'write_disposition':'WRITE_APPEND',
                        'external_table':False,
                        'autodetect':True,
                        'max_id_key':None,
                        'deferrable':False
                    }     
                }
            }
        }    
    }
}