from datetime import datetime,timedelta
from airflow.models import Variable
from helpers.utils.general_config import PathsDataLake
from helpers.apis.querys.bigquery.template_query import QuerysBigquery


# constantes de uso geral

PROJECT_ID="video-aulas-ed"
DATASET_ID_INCOMING="incoming"
DATASET_ID_RAW="raw"
DATASET_ID_TRUSTED="trusted"
LOCATION="us-central1"
SERVICE_ACCOUNT=Variable.get('SERVICE_ACCOUNT')
GCP_CONN_ID= 'gcp_conn_id'
GOOGLE_CLOUD_DEFAULT='google_cloud_default'
PYSPARK_FILE='gs://video-aulas-ed/script_submit_spark/layer_incoming/source_type_api/api_fornecedores/submit_file_config.py'
DEFAULT_RETRY=4

# constantes com os nomes das tabelas
API_SERVICOS_ORGAOS='api_servicos_orgaos'
# API_FORNECEDORES='api_fornecedores'
# API_ORGAOS='api_orgaos'

TABLE_NAME_LIST=[
    API_SERVICOS_ORGAOS,
    # API_FORNECEDORES,
    # API_ORGAOS
]

path_dl = PathsDataLake(change_file_type='csv', change_table_name=API_SERVICOS_ORGAOS, change_file_extension='csv', flow_technology='bigquery')
query_dl = QuerysBigquery(project_id=PROJECT_ID, layer_destination=DATASET_ID_RAW, 
                          layer_origin=DATASET_ID_INCOMING, table_name=API_SERVICOS_ORGAOS)

# constantes com endereços utilizados para a tabela api_servicos_orgaos
PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING=path_dl.change_file_path(change_layer=DATASET_ID_INCOMING)
PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW     =path_dl.change_file_path(change_layer=DATASET_ID_RAW)

# constantes com URIs de cada api
URI_API_SERVICOS_ORGAOS = 'http://compras.dados.gov.br/servicos/v1/servicos.csv'
# URI_API_ORGAOS          = 'http://compras.dados.gov.br/licitacoes/v1/orgaos.csv'
# URI_API_FORNECEDORES    = 'http://compras.dados.gov.br/fornecedores/v1/fornecedores.csv'


CALL_API_GOV = {
    "DAG_CONFIG":{
        'DAG_ID':'apis_gov_br_bigquery',
        'PROJECT_ID':PROJECT_ID,
        'DEFAULT_ARGS':{
            'owner':'Alison',
            'start_date':datetime(2023, 6, 9),
            'retries': 4,
            'retry_delay':timedelta(seconds=120),
            'wait_for_downstream': False,
            'depends_on_past':False,
        },
        'SCHEDULE_INTERVAL':'@daily',
        'CATCHUP':False,
        'TAGS':['api_gov', 'cloud-function', 'bigquery']
    },
    'TASK_CONFIG':{
        'PIPELINES_TABLES':{
            API_SERVICOS_ORGAOS:{
                'EXTRATION_API':{
                    'DELETE_FILE':{
                        'task_id':'delete_file_incoming',
                        'bucket_name':PROJECT_ID,
                        'prefix':PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING,
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },                    
                    "API_ACCESS":{
                        'task_id':'extration',
                        'method':'GET',
                        'http_conn_id':'http_conn_id_data_gov_br',
                        'endpoint':'formiga-cortadeira',
                        'data':{
                            'URL_API':URI_API_SERVICOS_ORGAOS,
                            'PROJECT_ID':PROJECT_ID,
                            'BUCKET_NAME':PROJECT_ID,
                            'PATH_FILE':PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING
                        },
                        'headers':{"Content-Type": "application/json"}
                    },
                    'CREATE_EXTERNAL_TABLE':{
                        'task_id':f'create_external_table_incoming',
                        'configuration':{
                            'query':{
                                'query':query_dl.create_external_table(PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING),
                                'useLegacySql':False,
                                'priority': 'BATCH'
                            }
                        },
                        'location':LOCATION,
                    },
                },
                'INCOMING_TO_RAW':{
                    'DELETE_FILE':{
                        'task_id':'delete_file_raw',
                        'bucket_name':PROJECT_ID,
                        'prefix':PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW,
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },  
                    'TRANSFER_FILE':{
                        'task_id':'move_file_incoming_to_raw',
                        'source_bucket':PROJECT_ID,
                        'source_object':PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING,
                        'destination_bucket':PROJECT_ID,
                        'destination_object':PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW,
                    },
                    'CREATE_EXTERNAL_TABLE':{
                        'task_id':f'create_external_table_raw',
                        'configuration':{
                            'query':{
                                'query':query_dl.create_external_table(PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW),
                                'useLegacySql':False,
                                'priority': 'BATCH'
                            }
                        },
                        'location':LOCATION,
                    },
                }
            }    
        }
    }
}