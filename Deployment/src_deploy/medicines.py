#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
#Data analysis


# In[4]:





# ## Loading data

# ## Pandas

# In[5]:


#partD_drug = pd.read_csv("../data/Drug_no_30_day.csv")
#partD_drug = pd.read_csv("/home/kiran/medicare/data/Raw_drug_data.csv")
cluster=MongoClient('mongodb+srv://m001-student:m001-mongodb-basics@sandbox.uughf.mongodb.net/?retryWrites=true&w=majority')

print("Connection to mongodb : success")

db=cluster['Medicare']

collection = db['drug_data']

partD_drug  = pd.DataFrame(list(collection.find()))

print("Raw data loaded for generating medicine columns")


# In[6]:


partD_drug .drop(["Unnamed: 0","_id"],axis = 1,inplace = True)


# In[7]:


# partD_drug.isnull().sum()


# In[8]:


# partD_drug.head(1)


# In[9]:


partD_drug = partD_drug.loc[:,["Prscrbr_NPI","Gnrc_Name","Tot_Clms","Tot_Day_Suply","Tot_Drug_Cst"]]
print("Prscrbr_NPI,Gnrc_Name,Tot_Clms,Tot_Day_Suply,Tot_Drug_Cst columns selected")


# In[10]:


partD_drug.rename(columns={'Prscrbr_NPI':'NPI'},inplace = True)


# In[11]:


# partD_drug.info()


# In[12]:


partD_drug.columns = ["NPI","Drug_Name","Tot_Clms","Tot_Day_Suply","Tot_Drug_Cst"]
print("Raw medicine data columns renamed")


# In[13]:


# partD_drug.head()


# In[14]:


partD_drug.columns


# ## Grouping data 

# In[15]:


# df = pd.crosstab(partD_drug.NPI,[partD_drug.Drug_Name,partD_drug.Tot_Clms,partD_drug.Tot_Day_Suply,partD_drug.Tot_Drug_Cst])
# df = pd.crosstab(partD_drug.NPI,partD_drug.Drug_Name)
df = partD_drug.groupby(["NPI","Drug_Name"]).agg({'Tot_Clms' : 'sum', 'Tot_Day_Suply' :  'sum'
                                                  ,"Tot_Drug_Cst" : 'sum'})

print("Raw data for medecine grouped on NPI and drug name")


# In[16]:


# df.index[0][1] + "_" + df.columns[0]


# In[17]:


# df.isnull().sum()


# In[18]:


# len(df.index)


# In[19]:


# df.index[34488]


# In[20]:


df.head(10)

#index =  NPI and Drug_Name 
# columns are Tot_clms and so on
#df.Tot_Clms.iloc[1]


# In[21]:


drug_n = []
with open('/home/kiran/medicare/data/drug_name_columns.txt', 'r') as file:
    [drug_n.append(line.strip()) for line in file.readlines()]
len(drug_n)

print("Number of medicine columns from ML model train data loaded")


# In[22]:


temp = pd.DataFrame(columns = drug_n)
print("Empty dataframe created with all medicine columns")


# In[27]:


npi_index = 0
drug_index = 0
row_ind = 0
flag = "start"
while True:
    #we will go through each index and with index[0] i.e NPI changes we will exit inner loop
    npi =  df.index[npi_index][0]
    ck = df.index[drug_index][0]
    
    #setting all row values to 0 , to avoid null 
    temp.loc[row_ind,:] = 0
    temp.loc[row_ind,"NPI"] = npi
    
    while npi == ck:
        #setting npi number to NPI column
        # temp.loc[drug_index,"NPI"] = ck
        
        #add values to dataframe if columns are present in list
        for c in range(3):
            col_name = df.index[drug_index][1] + "_" + df.columns[c]
            if col_name in drug_n:
                temp.loc[row_ind,col_name] = df.iloc[drug_index,c]
                temp.loc[row_ind,:]
        if drug_index < 34487:
            drug_index += 1 
            ck = df.index[drug_index][0]
        else:
            flag = "done"
            break
    if flag == "start":        
        npi_index  = drug_index  
        row_ind +=1 
    else:
        break

print("Transformation data")


# In[35]:


print("Checking null values")
print(temp.isnull().sum())


# In[34]:


temp.to_csv("/home/kiran/medicare/data/output/medicines_data.csv")
print("Dataframe with medicines columns save succesfull")

