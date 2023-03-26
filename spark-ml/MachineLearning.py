#!/usr/bin/env python
# coding: utf-8

# # Setup

# In[1]:


import pyspark

spark = (
    pyspark.sql.SparkSession.builder.appName("MachineLearning")
    .master("yarn")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.instances", "2")
    .getOrCreate()
)
spark


# In[2]:


from pyspark.sql import SparkSession
from pyspark import sql
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


# # JSON Object Classes

# In[3]:


class Category:
  def __init__(self, cat_pattern):
    self.depth1, self.depth2, self.depth3, self.depth4 = list(map(int, cat_pattern.split("/")))[:4]
  
  def __str__(self):
    return "{}/{}/{}/{}".format(self.depth1, self.depth2, self.depth3, self.depth4)


# In[4]:


class ProductStock:
  def __init__(self, id, price, quantity_sold, rating_average):
    self.id = id
    self.price = float(price)
    self.rating_avg = float(rating_average)
    self.qty_sold = int(quantity_sold)
  
  def __str__(self):
    return "{}, {}, {}, {}".format(self.id, self.price, self.qty_sold, self.rating_avg)

  @staticmethod
  def create_columns():
    return ['ID', 'Price', 'Sold', 'Rating']

  @staticmethod
  def parse_from_json(json_object: dict):
    try:
      id = json_object['id']
    except Exception:
      id = None

    try:
      price = json_object['price']
    except Exception:
      price = None

    try:
      quantity_sold = json_object['quantity_sold']['value']
    except Exception:
      quantity_sold = -1

    try:
      rating_average = json_object['rating_average']
    except Exception:
      rating_average = None
    


    return ProductStock(id, price, quantity_sold, rating_average)
    
  def create_tuple(self):
    return (self.id, self.price, self.qty_sold, self.rating_avg)


# # Load JSON data

# In[5]:


import time
import json
import happybase

local_time = time.time()
connection = happybase.Connection("node-master", 9090)
products_table = connection.table("products_table")

i = 0
products = []
for key, data in products_table.scan():
    i += 1
    if i % 10000 == 0:
        print(f"time = {round(time.time() - local_time)}, i={i}, key={key}")
    product_json = json.loads(data[b'product_info:json_string'].decode("utf-8-sig"))
    products.append(ProductStock.parse_from_json(product_json))


# In[6]:


connection.close()


# In[7]:


df = spark.createDataFrame(list(map(lambda ds: ds.create_tuple(), products)), ProductStock.create_columns())


# In[8]:


df.show()


# # Training Kmeans

# In[9]:


from pyspark.ml.feature import VectorAssembler
assemble=VectorAssembler(inputCols=['Price', 'Sold', 'Rating'], outputCol='features')
assembled_data=assemble.transform(df)


# In[10]:


from pyspark.ml.feature import StandardScaler
scale=StandardScaler(inputCol='features',outputCol='standardized')
data_scale=scale.fit(assembled_data)
data_scale_output=data_scale.transform(assembled_data)
data_scale_output.show()


# In[11]:


from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np
silhouette_score=[]
evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
                                metricName='silhouette', distanceMeasure='squaredEuclidean')

cost = np.zeros(10)

for i in range(2,10):
        
    KMeans_algo=KMeans(featuresCol='standardized', k=i)

    KMeans_fit=KMeans_algo.fit(data_scale_output)

    cost[i] = KMeans_fit.summary.trainingCost

    output=KMeans_fit.transform(data_scale_output)

    score=evaluator.evaluate(output)

    silhouette_score.append(score)

    print("Silhouette Score:",score)


# In[12]:


silhouette_score


# In[13]:


#Visualizing the silhouette scores in a plot
import matplotlib.pyplot as plt
fig, ax = plt.subplots(1,1, figsize =(8,6))
ax.plot(range(2,10),silhouette_score, color = '#BB2649')
ax.set_xlabel("k")
ax.set_ylabel("cost")


# In[14]:


# Plot the cost
df_cost = pd.DataFrame(cost[2:])
df_cost.columns = ["cost"]
new_col = [2,3,4,5,6,7,8, 9]
df_cost.insert(0, 'cluster', new_col)

import pylab as pl
pl.plot(df_cost.cluster, df_cost.cost, color='#BB2649')
pl.xlabel('Number of Clusters')
pl.ylabel('Score')
pl.title('Elbow Curve')

