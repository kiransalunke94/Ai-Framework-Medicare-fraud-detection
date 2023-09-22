#!/usr/bin/env python
# coding: utf-8

# In[2]:


import json
import pandas as pd
from time import sleep
from pandas import read_csv
from kafka.admin import NewTopic,KafkaAdminClient
from kafka import KafkaProducer
import time
import requests


# In[2]:


Topic = NewTopic(name="pay_data",num_partitions=3,replication_factor=1)


# In[3]:


admin = KafkaAdminClient(bootstrap_servers = 'localhost:9092')


# In[4]:


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# In[5]:


producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=json_serializer)


# In[6]:


df = pd.read_csv('../data/payment_data.csv')


# In[7]:


df.shape


# In[8]:


df.head(1)


# In[12]:


len(df)


# In[9]:


col = df.columns.to_list()
# for i,index in enumerate(col):
#     print(i,index)


# In[10]:


for i in range(len(df)):
    message = {
                col[0] : int(df.loc[i,col[0]]),col[1]: str(df.loc[i,col[1]]),
                col[2] : str(df.loc[i,col[2]]),col[3]: str(df.loc[i,col[3]]),
                col[4] : str(df.loc[i,col[4]]),col[5]: str(df.loc[i,col[5]]),
                col[6] : str(df.loc[i,col[6]]),col[7]: int(df.loc[i,col[7]]),
                col[8] : str(df.loc[i,col[8]]),col[9] : str(df.loc[i,col[9]]),
                col[11]: str(df.loc[i,col[12]]),col[12]: str(df.loc[i,col[12]]),
                col[13]: str(df.loc[i,col[13]]),col[14]: str(df.loc[i,col[14]]),
                col[15] : str(df.loc[i,col[15]]),col[16]: str(df.loc[i,col[16]]),
                col[17] : str(df.loc[i,col[17]]),col[18]: str(df.loc[i,col[18]]),
                col[19] : str(df.loc[i,col[19]]),col[20]: str(df.loc[i,col[20]]),
                col[21] : str(df.loc[i,col[21]]),col[22]: str(df.loc[i,col[22]]), 
                col[23] : str(df.loc[i,col[23]]),col[24]: str(df.loc[i,col[24]]),
                col[25] : str(df.loc[i,col[25]]),col[26]: str(df.loc[i,col[26]]),
                col[27] : str(df.loc[i,col[27]]),col[28]: str(df.loc[i,col[28]]),
                col[29] : str(df.loc[i,col[29]]),col[30]: str(df.loc[i,col[30]]),
                col[31] : str(df.loc[i,col[31]]),col[32] : str(df.loc[i,col[32]]),
                col[33]: str(df.loc[i,col[33]]),col[34]: str(df.loc[i,col[34]]),
                col[35]: str(df.loc[i,col[35]]),col[36]: str(df.loc[i,col[36]]),
                col[37] : str(df.loc[i,col[37]]),col[38]: str(df.loc[i,col[38]]),
                col[39] : str(df.loc[i,col[39]]),col[40]: str(df.loc[i,col[40]]),
                col[41] : str(df.loc[i,col[41]]),col[42]: float(df.loc[i,col[42]]),
                col[43] : str(df.loc[i,col[43]]),col[44]: str(df.loc[i,col[44]]),
                col[45] : str(df.loc[i,col[45]]),col[46]: str(df.loc[i,col[1]]),
                col[47] : str(df.loc[i,col[47]]),col[48]: str(df.loc[i,col[3]]),
                col[49] : str(df.loc[i,col[49]]),col[50]: str(df.loc[i,col[5]]),
                col[51] : str(df.loc[i,col[51]]),col[52]: str(df.loc[i,col[7]]),
                col[53] : str(df.loc[i,col[53]]),col[54] : str(df.loc[i,col[9]]),
                col[55]: str(df.loc[i,col[55]]),col[56]: str(df.loc[i,col[12]]),
                col[57]: str(df.loc[i,col[57]]),col[58]: str(df.loc[i,col[14]]),
                col[59] : str(df.loc[i,col[59]]),col[60]: str(df.loc[i,col[16]]),
                col[61] : str(df.loc[i,col[61]]),col[62]: str(df.loc[i,col[18]]),
                col[63] : str(df.loc[i,col[63]]),col[64]: str(df.loc[i,col[20]]),
                col[65] : str(df.loc[i,col[65]]),col[66]: str(df.loc[i,col[22]]), 
                col[67] : str(df.loc[i,col[67]]),col[68]: str(df.loc[i,col[1]]),
                col[69] : str(df.loc[i,col[69]]),col[70]: str(df.loc[i,col[3]]),
                col[71] : str(df.loc[i,col[71]]),col[72]: str(df.loc[i,col[5]]),
                col[73] : str(df.loc[i,col[73]]),col[74]: str(df.loc[i,col[7]]),
                col[75] : str(df.loc[i,col[75]]),col[76] : str(df.loc[i,col[9]]),
                col[77]: str(df.loc[i,col[77]]),col[78]: str(df.loc[i,col[12]]),
                col[79]: str(df.loc[i,col[79]]),col[80]: str(df.loc[i,col[14]]),
                col[81] : str(df.loc[i,col[81]]),col[82]: str(df.loc[i,col[16]]),
                col[83] : str(df.loc[i,col[83]]),col[84]: str(df.loc[i,col[18]]),
                col[85] : str(df.loc[i,col[85]]),col[86]: str(df.loc[i,col[20]]),
                col[87] : str(df.loc[i,col[87]]),col[88]: str(df.loc[i,col[22]]),
                col[89] : str(df.loc[i,col[85]]),col[90]: str(df.loc[i,col[20]]),
                col[90] : str(df.loc[i,col[87]]),col[91]: str(df.loc[i,col[22]])
    
    }  
                                                
    print(message)
    producer.send("pay_data",message)


# In[3]:


time.sleep(15)
print("Payment data dumped into database")
exit()

# In[11]:


# for i in range(len(df)):
#     message = {}
#     colum = 0
#     while colum < 92:
#         message[col[0]] = int(df.loc[i,col[0]])
# NOT possible since diff datatypes

