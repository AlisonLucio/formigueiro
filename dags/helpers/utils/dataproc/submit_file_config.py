from pyspark.sql import SparkSession
import unicodedata
import sys 
import pandas as pd
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, DateType



GCS_DATA_SOURCE_PATH=sys.argv[1]
GCP_DATA_OUTPUT_PATH=sys.argv[2]
APP_NAME=sys.argv[3]

print('-------------------------------------------------------------------------')
print(f'GCS_DATA_SOURCE_PATH ===  {GCS_DATA_SOURCE_PATH}')
print(f'GCP_DATA_OUTPUT_PATH ===  {GCP_DATA_OUTPUT_PATH}')
print('-------------------------------------------------------------------------')

def print_info(message, df):
    print(f'{message} ---- Quantidade total de linhas {df.count()}')
    print(f'{message} ---- Colunas da tabela: {df.columns}')
    print(f'{message} ---- Amostras dos dados: {df.show()}')
    print(f'{message} ---- Schema: {df.printSchema()}')

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
    spark.catalog.clearCache()
    
    if 'layer_incoming' in GCS_DATA_SOURCE_PATH and 'layer_raw' in GCP_DATA_OUTPUT_PATH:

        schema = StructType().add("checksum", StringType(), False)

        df = spark.read.options(header='True', delimiter=",", inferSchema='False', encoding='UTF-8')\
            .csv(GCS_DATA_SOURCE_PATH)
        
        df_incoming_hash = df.withColumn("checksum", F.xxhash64(*df.schema.names))

        try:
            # pegar checksum já existentes na raw
            # o checksum não pode estar atrelado a data de ingestão, se isso ocorrer a cada dia que o workflow rodar vai gerar uma nova data 
            # e consequentemente um novo hash. 
            # por isso devo primeiro gerar o hash e só depois adicionar a coluna com a data de engestão
            df_raw = spark.read.options(header='True', delimiter=",", inferSchema='False', encoding='UTF-8')\
                .schema(schema).csv(GCP_DATA_OUTPUT_PATH).select(F.col('checksum'))
            
            df_join = df_incoming_hash.join(df_raw, df_incoming_hash.checksum == df_raw.checksum, 'leftanti')\
                .drop(df_raw.checksum).withColumn("ingestion_date", F.current_date().cast(DateType()))
            
            df_join.write.mode("overwrite").options(header="True", inferSchema="False", delimiter=",").csv(GCP_DATA_OUTPUT_PATH)

            print_info('Já havia arquivos em raw para esta partição --- ', df_join)

        except:
            df_save = df_incoming_hash.withColumn("ingestion_date", F.current_date().cast(DateType()))

            df_save.write.mode("overwrite").options(header="True", inferSchema="False", delimiter=",").csv(GCP_DATA_OUTPUT_PATH)

            print_info('Não havia arquivos em raw para esta partição. Estes foram os primeiros --- ', df_save)
    
    else: 
        convertUDF = F.udf(lambda z: funcao_normalizar(z), StringType())

        df = spark.read.options(header='True', delimiter=",", inferSchema='True', encoding='UTF-8', escape='\\').csv(GCS_DATA_SOURCE_PATH)

        new_names=[]
        old_columns = df.schema.names
        for old in old_columns:
             new_names.append(old.lower().replace(" ", '_').replace('ç','c').replace('á','a')\
                                         .replace('ã','a').replace('é','e').replace('í','i')\
                                         .replace('ó','o').replace('õ','o').replace('ú','u'))

        df = reduce(lambda df, idx: df.withColumnRenamed(old_columns[idx], new_names[idx]), range(len(old_columns)), df)
        df = reduce(lambda df, idx: df.withColumn(new_names[idx], convertUDF(F.col(new_names[idx]))), range(len(new_names)), df)

        df.write.mode("overwrite").options(header='True', inferSchema='True', delimiter=',').csv(GCP_DATA_OUTPUT_PATH)

        print_info('else do raw_to_trusted ',df)

if __name__ == "__main__":
    func_run()