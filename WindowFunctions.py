#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('load_ext', 'nb_black')
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


# In[2]:


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("MyFirstTry")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()


# In[3]:


l = [
    (1, "sales", 4200),
    (2, "admin", 3100),
    (3, "sales", 4000),
    (4, "sales", 4000),
    (5, "admin", 2700),
    (6, "dev", 3400),
    (7, "dev", 5200),
    (8, "dev", 3700),
    (9, "dev", 4400),
    (10, "dev", 4400),
]

data = spark.createDataFrame(l, schema=["id", "dept", "salary"])
data.show()


# In[4]:


df = data.groupBy("dept").agg(
    F.expr("collect_list(salary)").alias("list_salary"),
    F.expr("avg(salary)").alias("average_salary"),
    F.expr("sum(salary)").alias("total_salary"),
)

df.show()


# In[5]:


windowSpec = Window.partitionBy("dept")

df = (
    data.withColumn("list_salary", F.collect_list(F.col("salary")).over(windowSpec))
    .withColumn("average_salary", F.avg(F.col("salary")).over(windowSpec))
    .withColumn("total_salary", F.sum(F.col("salary")).over(windowSpec))
)

df.show()


# In[6]:


windowSpec = Window.partitionBy("dept").orderBy(F.asc("salary"))

df = (
    data.withColumn("list_salary", F.collect_list(F.col("salary")).over(windowSpec))
    .withColumn("average_salary", F.avg(F.col("salary")).over(windowSpec))
    .withColumn("total_salary", F.sum(F.col("salary")).over(windowSpec))
)

df.show()


# In[7]:


df.collect()[2]["list_salary"]


# In[10]:


windowSpec = (
    Window.partitionBy("dept")
    .orderBy(F.asc("salary"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df = (
    data.withColumn("list_salary", F.collect_list(F.col("salary")).over(windowSpec))
    .withColumn("average_salary", F.avg(F.col("salary")).over(windowSpec))
    .withColumn("total_salary", F.sum(F.col("salary")).over(windowSpec))
)

df.show()


# In[11]:


df.collect()[2]["list_salary"]


# In[13]:


windowSpec = (
    Window.partitionBy("dept")
    .orderBy(F.asc("salary"))
    .rowsBetween(-1, Window.currentRow)
)

df = (
    data.withColumn("list_salary", F.collect_list(F.col("salary")).over(windowSpec))
    .withColumn("average_salary", F.avg(F.col("salary")).over(windowSpec))
    .withColumn("total_salary", F.sum(F.col("salary")).over(windowSpec))
)

df.show()


# In[14]:


windowSpec = Window.partitionBy("dept").orderBy(F.asc("salary"))

df = (
    data.withColumn("average_salary", F.avg(F.col("salary")).over(windowSpec))
    .withColumn("total_salary", F.sum(F.col("salary")).over(windowSpec))
    .withColumn("rank", F.rank().over(windowSpec))
    .withColumn("dense_rank", F.dense_rank().over(windowSpec))
    .withColumn("perc_rank", F.percent_rank().over(windowSpec))
)

df.show()


# In[ ]:




