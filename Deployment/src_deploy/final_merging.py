#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


drug_data = pd.read_csv("/home/kiran/medicare/data/output/drug_sort.csv")
print("Sorted drug data loaded")
pay_data = pd.read_csv("/home/kiran/medicare/data/output/payment_data_aggregated.csv")
print("Sorted payment data loaded")
medicines_data = pd.read_csv("/home/kiran/medicare/data/output/medicines_data.csv")
print("Sorted medicines data loaded")


# In[3]:


data = pd.merge(drug_data,pay_data,on="NPI",how = "left")
print("Drug data and payment data joined")


# In[4]:


data_final =  pd.merge(data,medicines_data,on="NPI",how = "left")
print("Drug , payment and medicine join done")


# In[7]:


data_final.drop(["Unnamed: 0.1","Unnamed: 0_x"],axis = 1,inplace = True)


# In[10]:


print("Total columns in final data frame : ",len(data_final.columns))

data_final['claim_max-mean'] = data_final['max_Total_claims'] - data_final['avg_Total_claims']

data_final['supply_max-mean'] = data_final['max_Tot_Day_Suply'] - data_final['avg_Tot_Day_Suply']

data_final['drug_max-mean'] = data_final['max_Tot_Drug_Cst'] - data_final['avg_Tot_Drug_Cst']

print("Claim, supply and drug max-mean column added ")

# In[12]:


data_final.to_csv("../data/output/processed_data.csv")
print("Processed data saved and ready for engineering")

