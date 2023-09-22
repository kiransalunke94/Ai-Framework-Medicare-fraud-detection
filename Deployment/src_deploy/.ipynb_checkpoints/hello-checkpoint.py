#!/usr/bin/env python
# coding: utf-8

# In[1]:

import sys
import pandas as pd
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import Row
#from pyspark.sql.functions import *
from pyspark.ml.feature import *
import pickle
#import functions
from pyspark.sql import functions
import warnings
warnings.filterwarnings("ignore")
import sys
from pyspark.sql.functions import col,isnan, when, count,sequence


spark = SparkSession.builder.appName("Dataprocess").getOrCreate()

df_payment= spark.read.csv(sys.argv[1],inferSchema=True,header = True)

df_payment.printSchema()




