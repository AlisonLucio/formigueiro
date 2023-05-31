from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
import unicodedata
import sys 
import pandas as pd
from functools import reduce
import pyspark.sql.functions as F



GCS_DATA_SOURCE_PATH=sys.argv[1]
GCP_DATA_OUTPUT_PATH=sys.argv[2]
APP_NAME=sys.argv[3]

print('-------------------------------------------------------------------------')
print(f'GCS_DATA_SOURCE_PATH ===  {GCS_DATA_SOURCE_PATH}')
print(f'GCP_DATA_OUTPUT_PATH ===  {GCP_DATA_OUTPUT_PATH}')
print('-------------------------------------------------------------------------')

def print_info(df):
    print(f'Quantidade total de linhas {df.count()}')
    print(f'Colunas da tabela: {df.columns}')
    print(f'Amostras dos dados: {df.show()}')
    print(f'Informações: {df.printSchema()}')

def funcao_normalizar(texto):
    string_velha = str(texto) \
                .lower()\
                .replace(' ','_')\
                .replace('/','-')\
                .replace('"','') \
                .replace('ç','c')\
                .replace('á','a')\
                .replace('ã','a')\
                .replace('é','e')\
                .replace('í','i')\
                .replace('ó','o')\
                .replace('õ','o')\
                .replace('ú','u')
    try:
        string_nova = ''.join(ch for ch in unicodedata.normalize('NFKD', string_velha)
            if not unicodedata.combining(ch))
        return string_nova
    except:
        return string_velha

def func_run():

    spark = SparkSession.builder.appName(f'{APP_NAME}_job_spark').getOrCreate()
    
    if 'layer_incoming' in GCS_DATA_SOURCE_PATH and 'layer_raw' in GCP_DATA_OUTPUT_PATH:

        df = spark.read.options(header='True', delimiter=",", inferSchema='False', encoding='UTF-8').csv(GCS_DATA_SOURCE_PATH)
        df_incoming_hash = df.withColumn("checksum", F.xxhash64(*df.schema.names))
        df_incoming_hash.write.mode("overwrite").options(header="True", inferSchema="False", delimiter=",").csv(GCP_DATA_OUTPUT_PATH)

        print_info(df_incoming_hash)
    
    else: 
        convertUDF = F.udf(lambda z: funcao_normalizar(z),F.StringType())

        df = spark.read.options(header='True', delimiter=",", inferSchema='True', encoding='UTF-8', escape='\\').csv(GCS_DATA_SOURCE_PATH)

        new_names=[]
        old_columns = df.schema.names
        for old in old_columns:
             new_names.append(old.lower().replace(" ", '_').replace('ç','c').replace('á','a')\
                                         .replace('ã','a').replace('é','e').replace('í','i').replace('ó','o')\
                                         .replace('õ','o').replace('ú','u'))

        df = reduce(lambda df, idx: df.withColumnRenamed(old_columns[idx], new_names[idx]), range(len(old_columns)), df)
        df = reduce(lambda df, idx: df.withColumn(new_names[idx], convertUDF(F.col(new_names[idx]))), range(len(new_names)), df)

        df.write.mode("overwrite").options(header='True', inferSchema='True', delimiter=',').csv(GCP_DATA_OUTPUT_PATH)

        print_info(df)

if __name__ == "__main__":
    func_run()