from datetime import datetime, timedelta
from airflow.models import Variable
from helpers.utils.dataproc.dataproc_config import (
    get_cluster_config,
    get_job_spark
    )
from helpers.utils.general_config import change_file_path, get_cluster_name


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
API_SERVICOS_ORGAOS='api_servicos_orgaos'
API_FORNECEDORES='api_fornecedores'
API_ORGAOS='api_orgaos'
# ----------------------------------------------------------------------------

# constantes com endereços utilizados para a tabela api_servicos_orgaos
PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING=change_file_path(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv")
PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW=change_file_path(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv")
PATH_SAVE_FILE_API_SERVICOS_ORGAOS_TRUSTED=change_file_path(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_SERVICOS_ORGAOS, change_file_extension="csv")

# constantes com endereços utilizados para a tabela api_orgaos
PATH_SAVE_FILE_API_ORGAOS_INCOMING=change_file_path(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv")
PATH_SAVE_FILE_API_ORGAOS_RAW=change_file_path(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv")
PATH_SAVE_FILE_API_ORGAOS_TRUSTED=change_file_path(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_ORGAOS, change_file_extension="csv")

# constantes com endereços utilizados para a tabela api_fornecedores
PATH_SAVE_FILE_API_FORNECEDORES_INCOMING=change_file_path(change_layer=DATASET_ID_INCOMING, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv")
PATH_SAVE_FILE_API_FORNECEDORES_RAW=change_file_path(change_layer=DATASET_ID_RAW, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv")
PATH_SAVE_FILE_API_FORNECEDORES_TRUSTED=change_file_path(change_layer=DATASET_ID_TRUSTED, change_file_type="csv", change_table_name=API_FORNECEDORES, change_file_extension="csv")

# constantes com URIs de cada api
URI_API_SERVICOS_ORGAOS = 'http://compras.dados.gov.br/servicos/v1/servicos.csv'
URI_API_ORGAOS = 'http://compras.dados.gov.br/licitacoes/v1/orgaos.csv'
URI_API_FORNECEDORES = 'http://compras.dados.gov.br/fornecedores/v1/fornecedores.csv'


TABLE_NAME_LIST = [
    API_SERVICOS_ORGAOS,
    API_ORGAOS,
    API_FORNECEDORES
]


CALL_API_GOV = {
    "DAG_CONFIG":{
        "DAG_ID":"apis_gov_br",
        "PROJECT_ID":PROJECT_ID,
        "DEFAULT_ARGS":{
            'owner':'Alison',
            'start_date':datetime(2023, 5, 28),
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
            API_SERVICOS_ORGAOS:{
                "CLOUD_FUNCTION":{
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
                'DELETE_FILE':{
                    'INCOMING':{
                        'bucket_name':PROJECT_ID,
                        'prefix':PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING,
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                },
                'DATAPROC_CONFIG':{
                    'INCOMING_TO_RAW':{
                        'CREATE_CLUSTER':{
                            'task_id':f'create_cluster_{API_SERVICOS_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'cluster_config': get_cluster_config(),
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING, table_name=API_SERVICOS_ORGAOS)
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_SERVICOS_ORGAOS}',
                            'job': get_job_spark(project_id= PROJECT_ID, layer=DATASET_ID_INCOMING,table_name=API_SERVICOS_ORGAOS, 
                                                 pyspark_file= PYSPARK_FILE, file_origem_path= PATH_SAVE_FILE_API_SERVICOS_ORGAOS_INCOMING,
                                                 file_destination_path=PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW),
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'incoming_delete_cluster_{API_SERVICOS_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING, table_name=API_SERVICOS_ORGAOS)
                        },
                    },
                    'RAW_TO_TRUSTED':{
                        'CREATE_CLUSTER':{
                            'task_id':f'raw_create_cluster_{API_SERVICOS_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'cluster_config': get_cluster_config(),
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW, table_name=API_SERVICOS_ORGAOS)
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_SERVICOS_ORGAOS}',
                            'job': get_job_spark(project_id= PROJECT_ID, layer=DATASET_ID_RAW,table_name=API_SERVICOS_ORGAOS, 
                                                 pyspark_file= PYSPARK_FILE, file_origem_path= PATH_SAVE_FILE_API_SERVICOS_ORGAOS_RAW,
                                                 file_destination_path=PATH_SAVE_FILE_API_SERVICOS_ORGAOS_TRUSTED),
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'incoming_delete_cluster_{API_SERVICOS_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW, table_name=API_SERVICOS_ORGAOS)
                        },
                    },
                },
                'TRUSTED_TO_BIGQUERY':{
                    'task_id':f'trusted_to_bigquery-{API_SERVICOS_ORGAOS}',
                    'bucket': PROJECT_ID,
                    'source_objects':f'{PATH_SAVE_FILE_API_SERVICOS_ORGAOS_TRUSTED}/part*',
                    'destination_project_dataset_table':f'{PROJECT_ID}.api.{API_SERVICOS_ORGAOS}',
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
                    'time_partitioning':None,
                    'cluster_fields':None,
                    'autodetect':True,
                    'location':LOCATION,
                    'impersonation_chain':SERVICE_ACCOUNT,
                    'result_retry':DEFAULT_RETRY,
                    'result_timeout': None
                }               
            },
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
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING, table_name=API_FORNECEDORES)
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_FORNECEDORES}',
                            'job': get_job_spark(project_id= PROJECT_ID, layer=DATASET_ID_INCOMING,table_name=API_FORNECEDORES, 
                                                 pyspark_file= PYSPARK_FILE, file_origem_path= PATH_SAVE_FILE_API_FORNECEDORES_INCOMING,
                                                 file_destination_path=PATH_SAVE_FILE_API_FORNECEDORES_RAW),
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'incoming_delete_cluster_{API_FORNECEDORES}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING, table_name=API_FORNECEDORES)
                        },
                    },
                    'RAW_TO_TRUSTED':{
                        'CREATE_CLUSTER':{
                            'task_id':f'raw_create_cluster_{API_FORNECEDORES}',
                            'project_id':PROJECT_ID,
                            'cluster_config': get_cluster_config(),
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW, table_name=API_FORNECEDORES)
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_FORNECEDORES}',
                            'job': get_job_spark(project_id= PROJECT_ID, layer=DATASET_ID_RAW,table_name=API_FORNECEDORES, 
                                                 pyspark_file= PYSPARK_FILE, file_origem_path= PATH_SAVE_FILE_API_FORNECEDORES_RAW,
                                                 file_destination_path=PATH_SAVE_FILE_API_FORNECEDORES_TRUSTED),
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'incoming_delete_cluster_{API_FORNECEDORES}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW, table_name=API_FORNECEDORES)
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
                    'time_partitioning':None,
                    'cluster_fields':None,
                    'autodetect':True,
                    'location':LOCATION,
                    'impersonation_chain':SERVICE_ACCOUNT,
                    'result_retry':DEFAULT_RETRY,
                    'result_timeout': None
                }               
            },
            API_ORGAOS:{
                "CLOUD_FUNCTION":{
                    'task_id':'extration',
                    'method':'GET',
                    'http_conn_id':'http_conn_id_data_gov_br',
                    'endpoint':'formiga-cortadeira',
                    'data':{
                        'URL_API':URI_API_ORGAOS,
                        'PROJECT_ID':PROJECT_ID,
                        'BUCKET_NAME':PROJECT_ID,
                        'PATH_FILE':PATH_SAVE_FILE_API_ORGAOS_INCOMING
                    },
                    'headers':{"Content-Type": "application/json"}
                },
                'DELETE_FILE':{
                    'INCOMING':{
                        'bucket_name':PROJECT_ID,
                        'prefix':PATH_SAVE_FILE_API_ORGAOS_INCOMING,
                        'gcp_conn_id':GCP_CONN_ID,
                        'impersonation_chain':SERVICE_ACCOUNT
                    },
                },
                'DATAPROC_CONFIG':{
                    'INCOMING_TO_RAW':{
                        'CREATE_CLUSTER':{
                            'task_id':f'incoming_create_cluster_{API_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'cluster_config': get_cluster_config(),
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING, table_name=API_ORGAOS)
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_ORGAOS}',
                            'job': get_job_spark(project_id= PROJECT_ID, layer=DATASET_ID_INCOMING,table_name=API_ORGAOS, 
                                                 pyspark_file= PYSPARK_FILE, file_origem_path= PATH_SAVE_FILE_API_ORGAOS_INCOMING,
                                                 file_destination_path=PATH_SAVE_FILE_API_ORGAOS_RAW),
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'incoming_delete_cluster_{API_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING, table_name=API_ORGAOS)
                        },
                    },
                    'RAW_TO_TRUSTED':{
                        'CREATE_CLUSTER':{
                            'task_id':f'raw_create_cluster_{API_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'cluster_config': get_cluster_config(),
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW, table_name=API_ORGAOS)
                        },
                        'SUBMIT_JOB_SPARK':{
                            'task_id':f'submit_job_spark_{API_ORGAOS}',
                            'job': get_job_spark(project_id= PROJECT_ID, layer=DATASET_ID_RAW,table_name=API_ORGAOS, 
                                                 pyspark_file= PYSPARK_FILE, file_origem_path= PATH_SAVE_FILE_API_ORGAOS_RAW,
                                                 file_destination_path=PATH_SAVE_FILE_API_ORGAOS_TRUSTED),
                            'region':LOCATION,
                            'project_id':PROJECT_ID
                        },
                        'DELETE_CLUSTER':{
                            'task_id':f'incoming_delete_cluster_{API_ORGAOS}',
                            'project_id':PROJECT_ID,
                            'region':LOCATION,
                            'cluster_name':get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW, table_name=API_ORGAOS)
                        },
                    },
                },
                'TRUSTED_TO_BIGQUERY':{
                    'task_id':f'trusted_to_bigquery-{API_ORGAOS}',
                    'bucket': PROJECT_ID,
                    'source_objects':f'{PATH_SAVE_FILE_API_ORGAOS_TRUSTED}/part*',
                    'destination_project_dataset_table':f'{PROJECT_ID}.api.{API_ORGAOS}',
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
                    'time_partitioning':None,
                    'cluster_fields':None,
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