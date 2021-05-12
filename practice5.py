#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import findspark
# findspark.init()


# In[ ]:


# from pyspark import SparkConf,SparkContext
# from pyspark.sql import SQLContext,SparkSession
# conf = (SparkConf().setMaster("local").setAppName("MyFirstCSVLoad"))


# In[ ]:


import pyspark


# In[ ]:


from pyspark import SparkConf
# from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[ ]:


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("MyFirstTry")
    spark = SparkSession         .builder         .config(conf = conf)         .getOrCreate()
#     sc = SparkContext.getOrCreate(conf = conf)
#     df = SQLContext(sc)


# In[ ]:


spark


# In[ ]:


schema = StructType(
[
    StructField(name = "city", dataType=StringType(),nullable = True),
    StructField(name = "country", dataType= StringType(),nullable = True),
    StructField(name = "counts", dataType =LongType(),nullable = False),
])


# In[ ]:


rows = [
    Row("Los Angless","United States",3),
    Row("New York","United States",1),
    Row("London","United Kingdom",1),
]


# In[ ]:


parallizeRows = spark.sparkContext.parallelize(rows)


# In[ ]:


df = spark.createDataFrame(parallizeRows,schema)


# In[ ]:


df.show()


# In[ ]:


import pyspark.sql.functions as F


# In[ ]:


df.select("country").show(1)


# In[ ]:


df.select(F.col("country")).show(1)


# In[ ]:


df.select("country","city").show(1)


# In[ ]:


df.select(F.expr("country as destination")).show(2)


# In[ ]:


df.select(F.expr("country as destination").alias("country")).show(5)


# In[ ]:


new_df = df.selectExpr("country as new_country","country")


# In[ ]:


new_df.show()


# In[ ]:


new_df2 = df.selectExpr("avg(counts)","count(distinct(country))")


# In[ ]:


new_df2.show()


# In[ ]:


df.select(F.expr("*"),F.lit(1).alias("one")).show()


# In[ ]:


df = df.withColumn("One",F.lit(1))


# In[ ]:


df.show()


# In[ ]:


df = df.withColumn("one",F.expr("One"))


# In[ ]:


df.columns


# In[ ]:


df = df.withColumnRenamed("one","ONE")


# In[ ]:


df.columns


# In[ ]:


df = df.drop("ONE")


# In[ ]:


df.columns


# In[ ]:


df.filter(F.col("counts") < 2).show(1)


# In[ ]:


df.where("counts < 2").show(2)


# In[ ]:


df.where(F.col("counts") <= 1).where(F.col("country") != "United States").show()


# In[ ]:


df.select("city").distinct().count()


# In[ ]:


df.sample(withReplacement = False, fraction = 1.0, seed = 5).count()


# In[ ]:


df2 = df.randomSplit([0.67,0.33], seed = 5)


# In[ ]:


print(df.count())
print(df2[0].count())
print(df2[1].count())


# In[ ]:


rows = [
    Row("Berlin","Germany",2),
    Row("Bucharest","Romania",2),
]
parallelizeRows = spark.sparkContext.parallelize(rows)
df2 = spark.createDataFrame(rows,schema)


# In[ ]:


df3 = df.union(df2)


# In[ ]:


df3.createOrReplaceTempView("new_df")


# In[ ]:


df3.show()


# In[ ]:


df3.sort("counts").show()


# In[ ]:


df3.orderBy("counts").show()


# In[ ]:


df3.orderBy(F.desc("counts")).show()


# In[ ]:


df.limit(3).show()


# In[ ]:




