#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler, StandardScaler, MinMaxScaler,PowerTransformer
import joblib
import pickle


# In[ ]:


df_ml = pd.read_csv("/home/kiran/medicare/data/output/processed_data.csv")
print("Processed data loaded for prediction")


# In[ ]:


df_ml.head(1)


# In[ ]:


#df_ml.drop(["Unnamed: 0","State","NPI"],axis =1,inplace= True)
df_ml.drop(["Unnamed: 0","State","City","First_name","last_org_name","Unnamed: 0_y"],axis =1,inplace= True)

df_ml = df_ml.round(2)


# In[ ]:


df_ml.head(1)


# In[ ]:


df_ml.isna().sum()


# ### Scaling

# In[ ]:


# df_ml.columns.values


# In[ ]:


scale_features = df_ml.columns.to_list()[2:15]
# scale_features.append("Total_payments")
scale_features


#  ### Power transformer scaler: 

# In[ ]:


pt = PowerTransformer()


# In[ ]:


df_pscale = df_ml.copy()
df_pscale[scale_features] = pd.DataFrame(
    pt.fit_transform(df_pscale.loc[:,scale_features]), columns=scale_features
)

print("Features scaled by power transformer")


# ### Scaler assignment

# In[ ]:


#copying scaled output to new variable for further processiing

df_scale = df_pscale.copy()


# In[ ]:


#FFilled missing value with most occuring speciality

df_scale["Speciality"].fillna(df_scale.Speciality.mode,inplace = True)
df_scale["Speciality"].isnull().sum()


# In[ ]:


# # make list with top 30 variables
# top_30 = [x for x in df_scale.Speciality.value_counts().sort_values(ascending=False).head(30).index]
# #top_30.append('anesthesiology')
# top_30


# In[ ]:


top_30 = [line.rstrip() for line in open('/home/kiran/medicare/data/top30.txt')]
top_30


# In[ ]:


# for all categorical variables we selected
def top_x(df2,variable,top_x_labels):
    
    for label in top_x_labels:
        df2[variable+'_'+label] = np.where(df_scale[variable]==label,1,0)


# In[ ]:


#encode Nighborhood into the 10 most frequent categories
top_x(df_scale,'Speciality',top_30)
# display data
df_scale.tail(2)

print("One hot encoding done")


# In[ ]:


df_scale.drop("Speciality",axis=1,inplace = True)


# In[ ]:


df_scale.head(1)


# In[ ]:


#Total columns 226 , fraud column at the end

df_scale.columns


# In[ ]:


npi_ = pd.DataFrame(df_scale['NPI'])


# In[ ]:


npi_.head()


# In[ ]:


npi_.shape


# In[ ]:


df_scale.drop("NPI",axis =1,inplace=True)


# In[ ]:





# In[6]:


#model_ = joblib.load("/home/kiran/medicare/src_deploy/model/rf_1.1.joblib")
#result = model.predict_and_evaluate(X_test, Y_test)
#print(result)
with open('/home/kiran/medicare/src_deploy/model/model_pkl' , 'rb') as f:
    model_ = pickle.load(f)

# In[ ]:


#result = model_.score(X_test, y_test)
#print(result)


# In[ ]:


#res=predict_and_evaluate(model_,X_test,y_test)


# In[ ]:


g_pred=model_.predict(df_scale)

print("Predictions done")


# In[ ]:


# g_pred


# In[ ]:


npi_["pred"] = g_pred


# In[ ]:


npi_.head()


# In[ ]:


npi_.pred.value_counts()


# In[ ]:


npi_ = npi_[npi_.pred == 1]


# In[ ]:

np.savetxt(r"/home/kiran/medicare/data/output/np.txt", npi_.NPI.values, fmt='%d')

npi_.NPI.to_csv("/home/kiran/medicare/data/output/NPI_FNF.csv")

print("NPI of fraud practioner saved")

