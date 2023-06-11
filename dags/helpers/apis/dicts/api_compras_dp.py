from datetime import datetime, timedelta
from airflow.models import Variable
from helpers.utils.dataproc.dataproc_config import (
    get_cluster_config
    )
from helpers.utils.general_config import PathsDataLake, get_cluster_name


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
# ----------------------------------------------------------------------------

# constantes com os nomes das tabelas
# API_SERVICOS_ORGAOS='api_servicos_orgaos'
API_FORNECEDORES='api_fornecedores'
# API_ORGAOS='api_orgaos'
# ----------------------------------------------------------------------------

# # constantes com endereços utilizados para a tabela api_servicos_orgaos
# PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING=change_file_path_incoming(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv")
# PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW=change_file_path_raw_and_trusted(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS)
# PATH_SAVE_FILE_API_SERVICOS_ORGAOS_TRUSTED=change_file_path_raw_and_trusted(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS)

# # constantes com endereços utilizados para a tabela api_orgaos
# PATH_SAVE_FILE_API_ORGAOS_INCOMING=change_file_path_incoming(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv")
# PATH_SAVE_FILE_API_ORGAOS_RAW=change_file_path_raw_and_trusted(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_ORGAOS)
# PATH_SAVE_FILE_API_ORGAOS_TRUSTED=change_file_path_raw_and_trusted(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_ORGAOS)

path_dl = PathsDataLake(change_file_type='csv', change_table_name=API_FORNECEDORES, change_file_extension='csv', flow_technology='dataproc')

# constantes com endereços utilizados para a tabela api_fornecedores
PATH_SAVE_FILE_API_FORNECEDORES_INCOMING=path_dl.change_file_path(change_layer=DATASET_ID_INCOMING)
PATH_SAVE_FILE_API_FORNECEDORES_RAW=path_dl.change_file_path(change_layer=DATASET_ID_RAW)
PATH_SAVE_FILE_API_FORNECEDORES_TRUSTED=path_dl.change_file_path(change_layer=DATASET_ID_TRUSTED)

# nomes de clusters 
CLUSTER_NAME_INCOMING = get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING, table_name=API_FORNECEDORES)
CLUSTER_NAME_RAW = get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW, table_name=API_FORNECEDORES)

# constantes com URIs de cada api
# URI_API_SERVICOS_ORGAOS = 'http://compras.dados.gov.br/servicos/v1/servicos.csv'
# URI_API_ORGAOS          = 'http://compras.dados.gov.br/licitacoes/v1/orgaos.csv'
URI_API_FORNECEDORES    = 'http://compras.dados.gov.br/fornecedores/v1/fornecedores.csv'


TABLE_NAME_LIST = [
    # API_SERVICOS_ORGAOS,
    # API_ORGAOS,
    API_FORNECEDORES
]


CALL_API_GOV = {
    "DAG_CONFIG":{
        "DAG_ID":"apis_gov_br_dataproc",
        "PROJECT_ID":PROJECT_ID,
        "DEFAULT_ARGS":{
            'owner':'Alison',
            'start_date':datetime(2023, 6, 8),
            'retries': 4,
            'retry_delay': timedelta(seconds=120),
            'wait_for_downstream': True,
            'depends_on_past': True, # mudar isso quando tudo estiver rodando ok
        },
        'SCHEDULE_INTERVAL':'@daily',
        'CATCHUP':True, # mudar isso quando tudo estiver rodando ok
        'TAGS':['api_gov', 'cloud-function', 'dataproc', 'bigquery']
    },
    "TASK_CONFIG":{
        "ZONE":LOCATION,
        "PIPELINES_TABLES":{
            API_FORNECEDORES:{
                "CLOUD_FUNCTION":{
                    'task_id':'extration',
                    'method':'GET',
                    'http_conn_id':'http_conn_id_data_gov_br',
                    'endpoint':'formiga-cortadeira',
                    'data':{
                        'URL_API':URI_API_FORNECEDORES,
                        'PROJECT_ID':PROJECT_ID,
                        'BUCKET_NAME':PROJECT_ID,
                        'PATH_FILE':PATH_SAVE_FILE_API_FORNECEDORES_INCOMING
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                'DELETE_FILE':{
                    'INCOMING':{
                        'bucket_name':PROJECT_ID,
                        'prefix':PATH_SAVE_FILE_API_FORNECEDORES_INCOMING,
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                },
                'DATAPROC_CONFIG':{
                    'INCOMING_TO_RAW':{
                        'CREATE_CLUSTER':{
                            'task_id':f'incoming_create_cluster_{API_FORNECEDORES}',
                            'project_id':PROJECT_ID,
                            'cluster_config': get_cluster_config(),
                            'region':LOCATION,
                            'cluster_name':CLUSTER_NAME_INCOMING
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_FORNECEDORES}',
                            'job':{
                                'reference': {'project_id':PROJECT_ID},
                                'placement': {'cluster_name': CLUSTER_NAME_INCOMING},
                                'pyspark_job':{
                                    'main_python_file_uri': PYSPARK_FILE,
                                    'args': [
                                        f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_API_FORNECEDORES_INCOMING}',
                                        f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_API_FORNECEDORES_RAW}',
                                        f'incoming_to_raw-{API_FORNECEDORES}',
                                        f'gs://{PROJECT_ID}/{path_dl.get_file_check_path(DATASET_ID_RAW)}'
                                    ]
                                    }
                            },
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'incoming_delete_cluster_{API_FORNECEDORES}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':CLUSTER_NAME_INCOMING
                        },
                    },
                    'RAW_TO_TRUSTED':{
                        'CREATE_CLUSTER':{
                            'task_id':f'raw_create_cluster_{API_FORNECEDORES}',
                            'project_id':PROJECT_ID,
                            'cluster_config': get_cluster_config(),
                            'region':LOCATION,
                            'cluster_name':CLUSTER_NAME_RAW
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_FORNECEDORES}',
                            'job':{
                                'reference':{"project_id": PROJECT_ID},
                                'placement':{'cluster_name': CLUSTER_NAME_RAW},
                                'pyspark_job':{
                                    'main_python_file_uri': PYSPARK_FILE,
                                    'args':[
                                        f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_API_FORNECEDORES_RAW}',
                                        f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_API_FORNECEDORES_TRUSTED}',
                                        f'raw_to_trusted-{API_FORNECEDORES}',
                                        ''
                                    ]
                                    },
                            },
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'raw_delete_cluster_{API_FORNECEDORES}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':CLUSTER_NAME_RAW
                        },
                    },
                },
                'TRUSTED_TO_BIGQUERY':{
                    'task_id':f'trusted_to_bigquery-{API_FORNECEDORES}',
                    'bucket': PROJECT_ID,
                    'source_objects':f'{PATH_SAVE_FILE_API_FORNECEDORES_TRUSTED}/part*',
                    'destination_project_dataset_table':f'{PROJECT_ID}.api.{API_FORNECEDORES}',
                    'source_format':'csv',
                    'compression':'GZIP',
                    'create_disposition':'CREATE_IF_NEEDED',
                    'write_disposition':'WRITE_APPEND',
                    'field_delimiter':',',
                    'quote_character':'"',
                    'ignore_unknown_values':True,
                    'allow_jagged_rows':False,
                    'encoding':'UTF-8',
                    'gcp_conn_id':'gcp_conn_id',
                    'time_partitioning': {
                        'field':'ingestion_date',
                        'type':'MONTH',
                        'expiration_ms': None,
                        'require_partition_filter': True
                    },
                    'cluster_fields': ('uf', 'municipio'),
                    'autodetect':True,
                    'location':LOCATION,
                    'impersonation_chain':SERVICE_ACCOUNT,
                    'result_retry':DEFAULT_RETRY,
                    'result_timeout': None
                }               
            },
        }    
    }
}