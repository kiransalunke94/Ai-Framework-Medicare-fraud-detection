#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd
from time import sleep
from pandas import read_csv
from kafka.admin import NewTopic,KafkaAdminClient
from kafka import KafkaProducer
import time
import requests
import numpy as np


# In[2]:


Topic = NewTopic(name="drug_data",num_partitions=3,replication_factor=1)


# In[3]:


admin = KafkaAdminClient(bootstrap_servers = 'localhost:9092')


# In[4]:


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# In[5]:


producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=json_serializer)


# In[6]:


df = pd.read_csv('../data/Raw_drug_data.csv')


# In[7]:


df.shape


# In[8]:


df.head(1)


# In[9]:


df.fillna(0,inplace=True)


# In[35]:


col = df.columns.to_list()


# In[36]:


for i in range(len(df)):
    message = {
                col[0] : int(df.loc[i,col[0]]),
                col[1]: int(df.loc[i,col[1]]),
                col[2] : str(df.loc[i,col[2]]),
                col[3]: str(df.loc[i,col[3]]),
                col[4] : str(df.loc[i,col[4]]),
                col[5]: str(df.loc[i,col[5]]),
                col[6] : str(df.loc[i,col[6]]),
                col[7]: str(df.loc[i,col[7]]),
                col[8] : str(df.loc[i,col[8]]),
                col[9] : str(df.loc[i,col[9]]),
                col[10] : str(df.loc[i,col[10]]),
                col[11]: float(df.loc[i,col[11]]),
                col[12]: float(df.loc[i,col[12]]),
                col[13]: float(df.loc[i,col[13]]),
                col[14]: float(df.loc[i,col[14]]),
                col[15] : str(df.loc[i,col[15]]),
                col[16]: str(df.loc[i,col[16]]),
                col[17] : str(df.loc[i,col[17]]),
                col[18]: str(df.loc[i,col[18]]),
                col[19] : str(df.loc[i,col[19]]),
                col[20]: str(df.loc[i,col[20]]),
                col[21] : str(df.loc[i,col[21]]),
                col[22]: str(df.loc[i,col[22]])   }  
                                                
    print(message)
    producer.send("drug_data",message)




time.sleep(15)
print("Drug data uploading successfull")
exit()

