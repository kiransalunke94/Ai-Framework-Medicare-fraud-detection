#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from time import sleep
# from pandas import read_csv
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


# In[2]:


def json_deserializer(data):
    return json.loads(data.decode('utf-8'))


# In[3]:


spark = SparkSession \
  .builder \
  .appName("pay_data_StrcturedStreaming") \
  .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector:10.0.0") \
  .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1') \
  .getOrCreate()


# In[4]:


spark.sparkContext.setLogLevel("ERROR")


# In[5]:


df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", 'localhost:9092') \
            .option("subscribe", 'pay_data') \
            .option("startingOffsets", 'latest') \
            .option("header","true") \
            .option("inferSchema","true") \
            .option("failOnDataLoss","false")\
            .load()

df.printSchema()


# In[6]:


schema= StructType().add('Unnamed: 0', IntegerType(), False) \
        .add('Change_Type', StringType(), False)\
        .add('Covered_Recipient_Type',StringType(), False) \
        .add('Teaching_Hospital_CCN',StringType(), False)\
        .add('Teaching_Hospital_ID',DoubleType(),False)\
        .add('Teaching_Hospital_Name',StringType(), False)\
        .add('Covered_Recipient_Profile_ID',DoubleType(),False)\
        .add('Covered_Recipient_NPI',IntegerType(),False)\
        .add('Covered_Recipient_First_Name', StringType() ,False)\
        .add('Covered_Recipient_Middle_Name', StringType() ,False)\
        .add('Covered_Recipient_Last_Name', StringType() ,False)\
        .add('Covered_Recipient_Name_Suffix', StringType() ,False)\
        .add('Recipient_Primary_Business_Street_Address_Line1', StringType() ,False)\
        .add('Recipient_Primary_Business_Street_Address_Line2', StringType() ,False)\
        .add('Recipient_City', StringType() ,False)\
        .add('Recipient_State', StringType() ,False)\
        .add('Recipient_Zip_Code',StringType() ,False)\
        .add('Recipient_Country',StringType() ,False)\
        .add('Recipient_Province',StringType() ,False)\
        .add('Recipient_Postal_Code',StringType() ,False)\
        .add('Covered_Recipient_Primary_Type_1',StringType(),False)\
        .add('Covered_Recipient_Primary_Type_2',StringType() ,False)\
        .add('Covered_Recipient_Primary_Type_3',StringType() ,False)\
        .add('Covered_Recipient_Primary_Type_4',DoubleType() ,False)\
        .add('Covered_Recipient_Primary_Type_5',StringType() ,False)\
        .add('Covered_Recipient_Primary_Type_6',StringType(),False)\
        .add('Covered_Recipient_Specialty_1',StringType(),False)\
        .add('Covered_Recipient_Specialty_2',StringType(),False)\
        .add('Covered_Recipient_Specialty_3',StringType(),False)\
        .add('Covered_Recipient_Specialty_4',StringType(),False)\
        .add('Covered_Recipient_Specialty_5',StringType(),False)\
        .add('Covered_Recipient_Specialty_6',StringType(),False)\
        .add('Covered_Recipient_License_State_code1',StringType(),False)\
        .add('Covered_Recipient_License_State_code2',StringType(),False)\
        .add('Covered_Recipient_License_State_code3',StringType() ,False)\
        .add('Covered_Recipient_License_State_code4',StringType() ,False)\
        .add('Covered_Recipient_License_State_code5',StringType() ,False)\
        .add('Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name',StringType(),False)\
        .add('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',StringType() ,False)\
        .add('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',StringType(),False)\
        .add('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',StringType(),False)\
        .add('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country',StringType(),False)\
        .add('Total_Amount_of_Payment_USDollars',FloatType() ,False)\
        .add('Date_of_Payment',StringType() ,False)\
        .add('Number_of_Payments_Included_in_Total_Amount',IntegerType(),False)\
        .add('Form_of_Payment_or_Transfer_of_Value',StringType(),False)\
        .add('Nature_of_Payment_or_Transfer_of_Value',StringType(),False)\
        .add('City_of_Travel',StringType(),False)\
        .add('State_of_Travel',StringType(),False)\
        .add('Country_of_Travel',StringType(),False)\
        .add('Physician_Ownership_Indicator',StringType(),False)\
        .add('Third_Party_Payment_Recipient_Indicator',StringType(),False)\
        .add('Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value',StringType(),False)\
        .add('Charity_Indicator',StringType(),False)\
        .add('Third_Party_Equals_Covered_Recipient_Indicator',StringType(),False)\
        .add('Contextual_Information',StringType(),False)\
        .add('Delay_in_Publication_Indicator',StringType(),False)\
        .add('Record_ID',StringType(),False)\
        .add('Dispute_Status_for_Publication',StringType(),False)\
        .add('Related_Product_Indicator',StringType(),False)\
        .add('Covered_or_Noncovered_Indicator_1',StringType(),False)\
        .add('Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1',StringType(),False)\
        .add('Product_Category_or_Therapeutic_Area_1',StringType(),False)\
        .add('Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',StringType(),False)\
        .add('Associated_Drug_or_Biological_NDC_1',StringType(),False)\
        .add('Associated_Device_or_Medical_Supply_PDI_1',DoubleType() ,False)\
        .add('Covered_or_Noncovered_Indicator_2',StringType(),False)\
        .add('Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2',StringType(),False)\
        .add('Product_Category_or_Therapeutic_Area_2',StringType(),False)\
        .add('Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2',StringType(),False)\
        .add('Associated_Drug_or_Biological_NDC_2',StringType(),False)\
        .add('Associated_Device_or_Medical_Supply_PDI_2',StringType() ,False)\
        .add('Covered_or_Noncovered_Indicator_3',StringType(),False)\
        .add('Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3',StringType(),False)\
        .add('Product_Category_or_Therapeutic_Area_3',StringType(),False)\
        .add('Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3',StringType(),False)\
        .add('Associated_Drug_or_Biological_NDC_3',StringType(),False)\
        .add('Associated_Device_or_Medical_Supply_PDI_3',StringType() ,False)\
        .add('Covered_or_Noncovered_Indicator_4',StringType(),False)\
        .add('Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4',StringType(),False)\
        .add('Product_Category_or_Therapeutic_Area_4',StringType(),False)\
        .add('Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4',StringType(),False)\
        .add('Associated_Drug_or_Biological_NDC_4',StringType(),False)\
        .add('Associated_Device_or_Medical_Supply_PDI_4',StringType(),False)\
        .add('Covered_or_Noncovered_Indicator_5',StringType(),False)\
        .add('Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5',StringType(),False)\
        .add('Product_Category_or_Therapeutic_Area_5',StringType(),False)\
        .add('Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5',StringType(),False)\
        .add('Associated_Drug_or_Biological_NDC_5',StringType(),False)\
        .add('Associated_Device_or_Medical_Supply_PDI_5',StringType() ,False)\
        .add('Program_Year',StringType(),False)\
        .add('Payment_Publication_Date',StringType() ,False) 


# In[7]:


df_1 = df.selectExpr("CAST(value AS STRING)")


# In[8]:


df_2 = df_1.select(from_json("value", schema).alias("data")).select("data.*")


# In[9]:


df3 = df_2.writeStream \
  .outputMode("update") \
  .option("checkpointLocation", getcwd()+"/checkpoint_dir") \
  .format("console") \
  .trigger(processingTime= "1 seconds") \
  .queryName("pay_data_receiver") \
  .start() 


# In[10]:
#Writing to hdfs 
df_2.writeStream \
    .format(source = "csv") \
    .option("path","hdfs://localhost:9000/kiran/medicare/pay_data") \
    .option("checkpointlocation","hdfs://localhost:9000/kiran/pay_data_checkpoint") \
    .start()

df_2.writeStream.format("com.mongodb.spark.sql.connector.MongoTableProvider") \
    .queryName("pay_dump") \
    .option("checkpointLocation","/tmp/pyspark7/") \
    .option("forceDeleteTempCheckpointLocation","true") \
    .option('spark.mongodb.connection.uri', "mongodb+srv://m001-student:m001-mongodb-basics@sandbox.uughf.mongodb.net/?retryWrites=true&w=majority") \
    .option("spark.mongodb.database", 'Medicare') \
    .option('spark.mongodb.collection', 'payment_data_test') \
    .trigger(continuous="1 seconds") \
    .outputMode("append") \
    .start()


# In[11]:


df3.awaitTermination()


# In[ ]:




