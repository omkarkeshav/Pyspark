#!/usr/bin/env python
# coding: utf-8

# In[21]:


import pyspark


# In[22]:


from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SQLContext
from itertools import islice
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


# In[23]:


import pyspark.sql.functions as F
import pyspark.sql.types as T


# In[24]:


import sys
import os


# In[25]:


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("MyFirstTry")
    spark = SparkSession         .builder         .config(conf = conf)         .getOrCreate()


# In[26]:


sc = SparkContext.getOrCreate(conf = conf)
df = SQLContext(sc)


# In[27]:


rdd1= sc.textFile('C://Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/test.csv').map(lambda line: line.split(","))


# In[28]:


rdd1 = rdd1.mapPartitionsWithIndex(lambda idx, it: (islice(it, 1,None) if idx == 0 else it))


# In[29]:


df1 = rdd1.toDF(['policyID', 'statecode', 'county', 'eq_site_limit'])


# In[30]:


targetDf = df1.withColumn('eq_site_limit', when(df1['eq_site_limit'
                              ] == 0, 'null'
                              ).otherwise(df1['eq_site_limit']))


# In[31]:


df1WithoutNullVal = targetDf.filter(targetDf.eq_site_limit != 'null')


# In[32]:


df1WithoutNullVal.show()


# In[33]:


rdd2= sc.textFile('C://Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/test1.csv').map(lambda line: line.split(","))


# In[34]:


rdd2 = rdd2.mapPartitionsWithIndex(
lambda idx, it: islice(it, 1, None) if idx == 0 else it
)


# In[35]:


df2 = rdd2.toDF(['policyID', 'zip', 'region', 'state'])


# In[47]:


innerjoineddf = df1WithoutNullVal.alias('a').join(df2.alias('b'),
col('b.policyID') == col('a.policyID').select([col('a' + xx) for xx in a.columns] + 
                                               [col('b.zip'), col('b.region'), col('b.state')]))


# In[43]:


innerjoineddf = df1WithoutNullVal.alias('a').join(df2.alias('b'),
col("b.policyID") == col("a.policyID")).select([col("a." +xx) for xx in a.columns] + [col("b.zip"), col("b.region"), col("b.state")])


# In[ ]:


# innerjoinddf = df1WithoutNullVal.select(F.expr('a')).join(df2.select(F.expr('b'))),
# col("b.policyID") == col("a.policyID").select([col("a." + xx) for xx in a.columns] + [col("b.zip"), col("b.region"), col("b.state")])


# In[44]:


innerjoinddf = df1WithoutNullVal.selectExpr('a').join(df2.selectExpr('b'),
col("b.policyID") == col("a.policyID")).select([col("a." +xx) for xx in a.columns] + [col("b.zip"), col("b.region"), col("b.state")])


# In[11]:


rows = [
    Row("2020-01-03"),
    Row("2020 01 10"),
    Row("2020 Jan 10"),
    Row("Sat, 11 Jan 2020"),
]

myrdd = spark.sparkContext.parallelize(rows)

schema = T.StructType(
    [
        T.StructField(name = "date_str",dataType=T.StringType(),nullable = True)
    ]
)
df = spark.createDataFrame(myrdd,schema)


# In[12]:


df.printSchema()


# In[13]:


df = df.withColumn(
    "date",
    F.when(
        F.to_date(F.col("date_str"),"yyyy-MM-dd").isNotNull(),
        F.to_date(F.col("date_str"),"yyyy-MM-dd"),
    ).otherwise(
        F.when(
            F.to_date(F.col("date_str"),"yyyy MM dd").isNotNull(),
            F.to_date(F.col("date_str"),"yyyy MM dd"),
        ).otherwise(
            F.when(
                F.to_date(F.col("date_str"),"yyyy MMM dd").isNotNull(),
                F.to_date(F.col("date_str"),"yyyy MMM dd"),
            ).otherwise(
                F.when(
                    F.to_date(F.col("date_str"),"E, dd MMMM yy").isNotNull(),
                    F.to_date(F.col("date_str"),"E, dd MMMM yy"),
                )
            ),
        ),
            
    ),
)


# In[14]:


df = df.withColumn(
    "timestamp",
    F.when(
        F.to_timestamp(F.col("date_str"),"yyyy-MM-dd").isNotNull(),
        F.to_timestamp(F.col("date_str"),"yyyy-MM-dd"),
    ).otherwise(
        F.when(
            F.to_timestamp(F.col("date_str"),"yyyy MM dd").isNotNull(),
            F.to_timestamp(F.col("date_str"),"yyyy MM dd"),
        ).otherwise(
            F.when(
                F.to_timestamp(F.col("date_str"),"yyyy MMM dd").isNotNull(),
                F.to_timestamp(F.col("date_str"),"yyyy MMM dd"),
            ).otherwise(
                F.when(
                    F.to_timestamp(F.col("date_str"),"E, dd MMMM yy").isNotNull(),
                    F.to_timestamp(F.col("date_str"),"E, dd MMMM yy"),
                )
            ),
        ),
            
    ),
)


# In[15]:


df.printSchema()


# In[16]:


df.show()


# In[17]:


df.withColumn("date_sub_10",F.date_sub("date",10)).show()


# In[18]:


df.withColumn("date_add_10",F.date_add("date",20)).show()


# In[ ]:


# difference in days between today and date columns


# In[19]:


df.withColumn("date_diff",F.datediff(F.current_date(),"date")).show()


# In[20]:


df.withColumn("year",F.year("date")).withColumn("month",F.month("date")).withColumn("day",F.dayofweek("date")).show()


# In[ ]:




