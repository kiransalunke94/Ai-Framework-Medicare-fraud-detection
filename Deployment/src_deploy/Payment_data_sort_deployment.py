#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pymongo
from pymongo import MongoClient


# In[2]:


# pay_data = pd.read_csv("/home/kiran/medicare/data/output/payment_data.csv")
cluster=MongoClient('mongodb+srv://m001-student:m001-mongodb-basics@sandbox.uughf.mongodb.net/?retryWrites=true&w=majority')
print("Connection to mongodb : success")
db=cluster['Medicare']

collection = db['payment_data']

print("Raw Payment data loaded")


# In[3]:


pay_data = pd.DataFrame(list(collection.find()))


# In[4]:


pay_data = pay_data[pay_data['Covered_Recipient_NPI'].notna()]
print("Not null rows of NPI selected")


# In[5]:


pay_data = pay_data.loc[:,["Covered_Recipient_NPI","Total_Amount_of_Payment_USDollars"]]
print("NPI and Payment data selected")


# In[6]:


pay_data.isnull().sum()


# In[7]:


pay_data.rename(columns = {"Covered_Recipient_NPI" : "NPI","Total_Amount_of_Payment_USDollars" : "Total_payments"},inplace=True)


# In[8]:


# data = pd.read_csv("../data/final_dataframe.csv")


# In[9]:


# data =  data.loc[:,["NPI","FRAUD"]]


# In[10]:


# pay = pd.merge(pay_data,data,on = "NPI",how = "inner")


# In[11]:


# pay.info()
# pay.rename(columns = {"NPI" : "Covered_Recipient_NPI"},inplace=True)


# In[12]:


# pay.drop("FRAUD",axis = 1 , inplace = True)


# In[13]:


pay = pay_data.groupby("NPI").sum()
print("Raw payment data grouped on NPI aggregated sum done on Payments")


# In[ ]:





# In[14]:


pay.info()


# In[15]:


pay.to_csv("/home/kiran/medicare/data/output/payment_data_aggregated.csv")
print("Aggregated data saved")
cluster.close()
print("CLuster connection closed")
