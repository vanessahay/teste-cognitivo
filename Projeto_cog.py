#!/usr/bin/env python
# coding: utf-8

# Case técnico
# Preparação de ambiente: Subi uma instancia EC2 ubuntu, instalei: Spark, python, jupyter e scala
# O codigo abaixo foi desenvolvido e testado no Jupyter e depois exportado o código.

import findspark
findspark.init('/home/ubuntu/spark-3.0.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.types import *
import json
sc = SparkContext.getOrCreate();
sqlContext = SQLContext(sc)
spark = SparkSession.builder.master("local[2]").config('spark.cores.max', '3')     .config('spark.executor.memory', '2g')     .config('spark.executor.cores', '2')     .config('spark.driver.memory','1g')     .getOrCreate()
schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("create_date", TimestampType(), True),
        StructField("update_date", TimestampType(), True)])


# Ingestão da base:
DF_parquet = spark.read.csv("/home/ubuntu/data/input/users/load.csv",header=True,schema=schema)

#req1: Converter o arquivo para um formato colunar de rapida leitura. Formato escolhido: parquet 
DF_parquet.write.mode('overwrite').parquet('/home/ubuntu/data/output/Users.parquet')

#Testando a ingestao
DF_parquet.count()
DF_parquet.show()


#req2: Retirar as linhas duplicadas mantendo a linha com a ultima data de atualização 
DF_dedup = DF_parquet.orderBy('id','update_date',ascending = False).coalesce(1).dropDuplicates(subset=['id'])


# Teste deduplicação]:
DF_dedup.count()
DF_dedup.show()


# persistindo o dado 
DF_dedup.write.mode('overwrite').parquet('/home/ubuntu/data/output/Users_dedup.parquet')


#req 3: converter os types do schema de acordo com o arquivo json 
with open("/home/ubuntu/config/types_mapping.json") as f:
     json_schema = json.load(f)
     for d in json_schema:
        df_convert = DF_dedup.withColumn(d,col(d).cast(json_schema[d]))


# Teste
df_convert.show()


# Persistindo do DF final
df_convert.write.mode('overwrite').parquet('/home/ubuntu/data/output/Users_final.parquet')







