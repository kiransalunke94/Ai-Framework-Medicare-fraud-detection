#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from time import sleep
from pandas import read_csv
from kafka.admin import NewTopic,KafkaAdminClient
from kafka import KafkaConsumer
import time
from os import getcwd
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import udf,from_json,col
from pyspark.sql.types import StringType,StructType,IntegerType,DoubleType,FloatType,ArrayType,StructField,TimestampType,DateType
# from pyspark.sql.functions import format_number, dayofmonth, hour, dayofyear,month, year, weekofyear, date_format,to_date
#from consumer import consumer_task # user-defined module
import warnings
warnings.filterwarnings('ignore')

from kafka import KafkaConsumer
import json
import joblib
import numpy as np
from kafka import TopicPartition
import warnings
warnings.filterwarnings('ignore')
# In[2]:


def json_deserializer(data):
    return json.loads(data.decode('utf-8'))


# In[3]:


# consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
#             auto_offset_reset="latest",
#             group_id="group-1",
#             value_deserializer=json_deserializer)


# In[4]:


# topic = TopicPartition('drug_data',0)
# consumer.assign([topic])


# In[5]:


# for message in consumer:
#     claim = message.value
#     A = claim["Prscrbr_NPI"]
#     print(A)
    


# In[ ]:





# In[6]:


spark = SparkSession \
  .builder \
  .appName("Medicare_StrcturedStreaming") \
  .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector:10.0.0") \
  .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1') \
  .getOrCreate()


# In[7]:


spark.sparkContext.setLogLevel("ERROR")


# In[8]:


# BOOTSTRAP_SERVERS = 'localhost:9092'


# In[9]:


df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", 'localhost:9092') \
            .option("subscribe", 'drug_data') \
            .option("startingOffsets", 'latest') \
            .option("header","true") \
            .option("inferSchema","true") \
            .option("failOnDataLoss","False") \
            .load()

df.printSchema()


# In[10]:


schema = StructType().add('Unnamed: 0',IntegerType(), True)\
    .add('Prscrbr_NPI', IntegerType(), True)\
    .add('Prscrbr_Last_Org_Name', StringType(), True)\
    .add('Prscrbr_First_Name', StringType(), True)\
    .add('Prscrbr_City', StringType(), True)\
    .add('Prscrbr_State_Abrvtn', StringType(), True)\
    .add('Prscrbr_State_FIPS', StringType(), True)\
    .add('Prscrbr_Type', StringType(), True)\
    .add('Prscrbr_Type_Src', StringType(), True)\
    .add('Brnd_Name', StringType(), True)\
    .add('Gnrc_Name', StringType(), True)\
    .add('Tot_Clms', FloatType(), True)\
    .add('Tot_30day_Fills', FloatType(), True)\
    .add('Tot_Day_Suply', FloatType(), True)\
    .add('Tot_Drug_Cst', FloatType(), True)\
    .add('Tot_Benes', StringType(), True)\
    .add('GE65_Sprsn_Flag', StringType(), True)\
    .add('GE65_Tot_Clms', StringType(), True)\
    .add('GE65_Tot_30day_Fills', StringType(), True)\
    .add('GE65_Tot_Drug_Cst', StringType(), True)\
    .add('GE65_Tot_Day_Suply', StringType(),True)\
    .add('GE65_Bene_Sprsn_Flag', StringType(), True)\
    .add('GE65_Tot_Benes', StringType(), True)


# In[11]:


df_1 = df.selectExpr("CAST(value AS STRING)")


# In[12]:


df_2 = df_1.select(from_json("value", schema).alias("data")).select("data.*")


# In[13]:


# df_2.createOrReplaceTempView("drug")
# row = spark.sql("select * from drug")
# row


# In[14]:


df3 = df_2.writeStream \
  .outputMode("update") \
  .option("checkpointLocation", getcwd()+"/checkpoint_dir") \
  .format("console") \
  .trigger(processingTime= "1 seconds") \
  .queryName("drug_data_receiver") \
  .start() 


# In[15]:


df_2.writeStream.format("com.mongodb.spark.sql.connector.MongoTableProvider") \
    .queryName("Dump") \
    .option("checkpointLocation","/tmp/pyspark7/") \
    .option("forceDeleteTempCheckpointLocation","true") \
    .option('spark.mongodb.connection.uri', "mongodb+srv://m001-student:m001-mongodb-basics@sandbox.uughf.mongodb.net/?retryWrites=true&w=majority") \
    .option("spark.mongodb.database", 'Medicare') \
    .option('spark.mongodb.collection', 'drug_data') \
    .trigger(continuous="1 seconds") \
    .outputMode("append") \
    .start()


# In[ ]:





# In[17]:


df3.awaitTermination()

