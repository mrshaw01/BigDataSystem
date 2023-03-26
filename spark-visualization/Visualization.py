#!/usr/bin/env python
# coding: utf-8

# # Setup

# In[ ]:


import pyspark
from pyspark.sql import SparkSession
from pyspark import sql
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

import pyspark

spark = (
    pyspark.sql.SparkSession.builder.appName("Visualization")
    .master("yarn")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.instances", "2")
    .getOrCreate()
)
spark


# # JSON Classes

# In[ ]:


class Category:
  def __init__(self, cat_pattern):
    self.depth1, self.depth2, self.depth3, self.depth4 = list(map(int, cat_pattern.split("/")))[:4]
  
  def __str__(self):
    return "{}/{}/{}/{}".format(self.depth1, self.depth2, self.depth3, self.depth4)


# In[ ]:


class ProductStock:
  def __init__(self, id, name, price, discount_rate, rating_average, inventory_status, productset_id, quantity_sold, shippable, primary_category_path):
    self.id = id
    self.name = name
    self.price = float(price)
    self.discount_rate = float(discount_rate)
    self.rating_avg = float(rating_average)
    self.inventory = inventory_status
    self.prod_id = int(productset_id)
    self.qty_sold = int(quantity_sold)
    self.shippable = shippable
    self.category = Category(primary_category_path)
  
  def __str__(self):
    return "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}".format(self.id, self.name, self.price, self.discount_rate, self.rating_avg, self.inventory, self.prod_id, self.qty_sold, self.shippable, self.category)

  @staticmethod
  def create_columns():
    return ['ID', 'Name', 'Price', 'Discount', 'Rating', 'Inventory', 'Product', 'Sold', 'Shippable', 'Category_1', 'Category_2', 'Category_3', 'Category_4']

  @staticmethod
  def parse_from_json(json_object: dict):
    try:
      id = json_object['id']
    except Exception:
      id = None

    try:
      name = json_object['name']
    except Exception:
      name = None

    try:
      price = json_object['price']
    except Exception:
      price = None

    try:
      discount_rate = json_object['discount_rate']
    except Exception:
      discount_rate = None

    try:
      rating_average = json_object['rating_average']
    except Exception:
      rating_average = None

    try:
      inventory_status = json_object['inventory_status']
    except Exception:
      inventory_status = None



    try:
      productset_id = json_object['productset_id']
    except Exception:
      productset_id = None


    try:
      quantity_sold = json_object['quantity_sold']['value']
    except Exception:
      quantity_sold = -1


    try:
      shippable = json_object['shippable']
    except Exception:
      shippable = None


    try:
      primary_category_path = json_object['primary_category_path']
    except Exception:
      primary_category_path = None
    


    return ProductStock(id, name, price, discount_rate, rating_average, inventory_status, productset_id, quantity_sold, shippable, primary_category_path)
    
  def create_tuple(self):
    return (self.id, self.name, self.price, self.discount_rate, self.rating_avg, self.inventory, self.prod_id, self.qty_sold, self.shippable, self.category.depth1, self.category.depth2, self.category.depth3, self.category.depth4)


# # Load data from Hbase

# In[ ]:


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
    product_json = json.loads(data[b'product_info:json_string'].decode("utf-8-sig"))
    products.append(ProductStock.parse_from_json(product_json))
connection.close()
df = spark.createDataFrame(list(map(lambda ds: ds.create_tuple(), products)), ProductStock.create_columns())


# In[ ]:


df.show()


# In[ ]:


df.count()


# In[ ]:


df = df.toPandas()


# # Overview

# In[ ]:


import pandas as pd
from pandas_profiling import ProfileReport

df_train = df

profile = ProfileReport(
    df_train
)
profile


# # Category Tree

# In[ ]:


import time
import json
import happybase

local_time = time.time()
connection = happybase.Connection("node-master", 9090)
categories_table = connection.table("categories_table")

i = 0
categories = []
for key, data in categories_table.scan():
    i += 1
    data_keys = [x.decode("utf-8-sig").replace("category_info:", "") for x in data.keys()]
    data_values = [y.decode("utf-8-sig") for y in data.values()]
    
    category_json = dict(zip(data_keys, data_values)) 
    category_json["id"] = key.decode("utf-8-sig")
    categories.append(category_json)
print(categories[-1])
connection.close()


# In[ ]:


parents = []
children = []
rela = []

for c in categories[:200]:
    if (c['parent_id'], c['id']) not in rela:
        rela.append((c['parent_id'], c['id']))
        parents.append(c['parent_id'])
        children.append(c['id'])


# In[ ]:


# libraries
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
 
# Build a dataframe with your connections
plt.figure(figsize=(20, 20))
dat = pd.DataFrame({ 'parent':parents, 'child':children})
 
# Build your graph
G=nx.from_pandas_edgelist(dat, 'parent', 'child')
 

# Custom the labels:
nx.draw(G, with_labels=True, node_size=1500, node_color='#BB2649', font_size=10, font_color="white", font_weight="bold")
# plt.show()

# # Spectral
# nx.draw(G, with_labels=True, node_size=2000, node_color="skyblue", font_size=10, font_color="black", font_weight="bold",pos=nx.spectral_layout(G))
# plt.show()


# # Compare two Category Tree

# In[ ]:


parents = []
children = []
rela = []

for c in categories[:200]:
    if (c['parent_id'], c['id']) not in rela:
        rela.append((c['parent_id'], c['id']))
        parents.append(c['parent_id'])
        children.append(c['id'])


# In[ ]:


# libraries
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
 
# Build a dataframe with your connections
plt.figure(figsize=(10, 10))
dat = pd.DataFrame({ 'parent':parents, 'child':children})
 
# Build your graph
G=nx.from_pandas_edgelist(dat, 'parent', 'child')
 

# Custom the labels:
nx.draw(G, with_labels=True, node_size=1500, node_color='#BB2649', font_size=10, font_color="white", font_weight="bold")
# plt.show()

# Spectral
# nx.draw(G, with_labels=True, node_size=2000, node_color="#BB2649", font_size=10, font_color="white", font_weight="bold",pos=nx.spectral_layout(G))
# plt.show()


# # Numerical Variables

# In[ ]:


df = df.replace(-1, np.nan)


# In[ ]:


df.head()


# In[ ]:


g=sns.PairGrid(df, vars=['Price', 'Rating', 'Sold'], diag_sharey=False, corner=True)
def lower(*args, **kwargs):
    kwargs['color'] = "#BB2649"
    sns.kdeplot(*args, fill=True, **kwargs)
def diag(*args, **kwargs):
    kwargs['color'] = "#BB2649"
    sns.distplot(*args, hist=True, kde=True, rug=False, norm_hist=False, **kwargs)
g.map_lower(lower)
g.map_diag(diag)

