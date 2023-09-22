#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pickle
import warnings
warnings.filterwarnings("ignore")
import pymongo
from pymongo import MongoClient


# ## Loading data

# In[2]:

print("loading data")
#df = pd.read_csv("/home/kiran/medicare/data/Raw_drug_data.csv")

cluster=MongoClient('mongodb+srv://m001-student:m001-mongodb-basics@sandbox.uughf.mongodb.net/?retryWrites=true&w=majority')
print("Connection to mongodb : success")

db=cluster['Medicare']

collection = db['drug_data']

df = pd.DataFrame(list(collection.find()))


# In[3]:

print("drug data loaded successfully")


# ## Data preprocessing

# ### Processing drug dataframe

# # Renaming column names of drug dataframe

# In[4]:

print("Processing drug dataframe")
df.rename(columns={"Tot_Clms":"Total_claims","Prscrbr_First_Name":"First_name","Prscrbr_City":"City","Prscrbr_State_Abrvtn":"State","Gnrc_Name":"Drug_name","Prscrbr_NPI":"NPI","Prscrbr_Last_Org_Name":"last_org_name"},inplace=True)


# In[5]:





# In[6]:


df.drop(["Unnamed: 0","_id"],axis=1,inplace=True)


# In[7]:


df.rename(columns={"Prscrbr_Type":"Speciality"},inplace=True)


# ### Grouping all rows on npi with sum , mean and max for drug values

# In[8]:

print("Grouping all rows on npi with sum , mean and max for drug values")
group_cols = ['NPI']

agg_dict = {'Tot_Drug_Cst':['sum','mean','max'],
           'Total_claims':['sum','mean','max'],
           'Tot_Day_Suply':['sum','mean','max'],
           'Tot_30day_Fills':['sum','mean','max']
           }


# In[9]:


df_agg = df.groupby(group_cols).agg(agg_dict).astype(float)
print("Raw drug data grouped and aggregated with max,mean and sum columns")


# In[11]:

print("shape of a aggregated dataframe")
print(df_agg.shape)


# In[46]:


df_agg.rename(columns={"(Tot_Drug_Cst, max)":"max_Tot_Drug_Cst","(Tot_Drug_Cst, sum)":"sum_Tot_Drug_Cst","(Tot_Drug_Cst, mean)":"avg_Tot_Drug_Cst","(Total_claims, max)":"max_Total_claims","(Total_claims, sum)":"sum_Total_claims","(Total_claims, mean)":"avg_Total_claims","(Tot_Day_Suply, max)":"max_Tot_Day_Suply","(Tot_Day_Suply, sum)":"sum_Tot_Day_Suply","(Tot_Day_Suply, mean)":"avg_Tot_Day_Suply","(Tot_30day_Fills, max)":"max_Tot_30day_Fills","(Tot_30day_Fills, sum)":"sum_Tot_30day_Fills","(Tot_30day_Fills, mean)": "avg_Tot_30day_Fills"}, inplace=True)


# In[47]:

print("Column names renamed")


# In[48]:



# ### Selecting string columns from drug dataframe

# In[14]:


df["NPI"].unique()


# In[15]:


df_names = df.loc[:,['NPI','City','State','First_name','last_org_name','Speciality']]
                                                


# In[16]:


df_new_names=df_names.drop_duplicates()


# In[17]:

print("selecting string columns")



# ### Joining string and aggregated dataframe

# In[50]:

print("Joining string and aggregated dataframe")
df_join=pd.merge(df_new_names,df_agg, how='left' ,on='NPI')


# In[51]:



# ### Saving dataframe to csv

# In[27]:


df_join.to_csv("sort_drug.csv")


# In[ ]:


#df_join.rename(columns={"('Tot_Drug_Cst','max')":"max_Tot_Drug_Cst","('Tot_Drug_Cst','sum')":"sum_Tot_Drug_Cst","('Tot_Drug_Cst','mean')":"avg_Tot_Drug_Cst","('Total_claims','max')":"max_Total_claims","('Total_claims','sum')":"sum_Total_claims","('Total_claims','mean')":"avg_Total_claims","('Tot_Day_Suply','max')":"max_Tot_Day_Suply","('Tot_Day_Suply','sum')":"sum_Tot_Day_Suply","('Tot_Day_Suply,mean')":"avg_Tot_Day_Suply","('Tot_30day_Fills','max')":"max_Tot_30day_Fills","('Tot_30day_Fills','sum')":"sum_Tot_30day_Fills","('Tot_30day_Fills','mean')": "avg_Tot_30day_Fills"}, inplace=True)


# In[54]:


df_final_drug=pd.read_csv("sort_drug.csv")


# In[55]:



# In[56]:


df_final_drug.rename(columns={"('Tot_Drug_Cst', 'max')":"max_Tot_Drug_Cst","('Tot_Drug_Cst', 'sum')":"sum_Tot_Drug_Cst","('Tot_Drug_Cst', 'mean')":"avg_Tot_Drug_Cst","('Total_claims', 'max')":"max_Total_claims","('Total_claims', 'sum')":"sum_Total_claims","('Total_claims', 'mean')":"avg_Total_claims","('Tot_Day_Suply', 'max')":"max_Tot_Day_Suply","('Tot_Day_Suply', 'sum')":"sum_Tot_Day_Suply","('Tot_Day_Suply', 'mean')":"avg_Tot_Day_Suply","('Tot_30day_Fills', 'max')":"max_Tot_30day_Fills","('Tot_30day_Fills', 'sum')":"sum_Tot_30day_Fills","('Tot_30day_Fills', 'mean')": "avg_Tot_30day_Fills"}, inplace=True)


# # In[58]:

print("final sorted drug dataframe")
print(df_final_drug.info())


# # In[59]:


df_final_drug.to_csv("/home/kiran/medicare/data/output/drug_sort.csv")
print("Drug data frame sorted and saved ")
cluster.close()
print("CLuster connection closed")
# In[ ]:




