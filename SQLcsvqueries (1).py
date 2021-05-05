#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql.functions import col


# In[17]:


from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import monotonically_increasing_id
# from pyspark.sql.functions import col,lit
from pyspark.sql.window import Window as W
# from pyspark.sql import functions as F
from pyspark.sql.window import Window


# In[18]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkExamples").getOrCreate()


# In[19]:


empdf = spark.read.format("csv").options(header=True,inferschema = True).load("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/emp.csv")


# In[7]:


empdf.show()


# In[8]:


deptdf = spark.read.format("csv").options(header='true', inferschema='true',truncate='true').load("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/dept.csv")


# In[9]:


deptdf.show(truncate =False)


# In[10]:


# df.withColumn("name",col("name").cast("String")).show()


# In[12]:


empdf.createOrReplaceTempView("EMP")


# In[13]:


deptdf.createOrReplaceTempView("DEPT")


# In[15]:


joinDF = spark.sql("select emp_id,name,salary,dept_name,dept_id from EMP e, DEPT d  where e.emp_dept_id == d.dept_id")


# In[16]:


joinDF.show(truncate=False)


# In[20]:


df1 = joinDF.withColumn("idx", monotonically_increasing_id())


# In[21]:


windowSpec = W.orderBy("idx")


# In[22]:


df2 = df1.withColumn("idx",row_number().over(windowSpec))


# In[23]:


df2.show(truncate=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# DDL = "emp_id INTEGER, name STRING, broken STRING"
# df = spark.read.csv("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/emp.csv", header=True, schema=DDL, enforceSchema=True, 
#                     columnNameOfCorruptRecord='broken')
# print(df.show())


# In[ ]:


# df = spark.read.option("mode", "PERMISSIVE").schema("emp_id Integer, name String").csv("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/emp.csv")


# In[ ]:


# df = spark.read.option('inferschema' = True).schema("emp_id Integer, name String").csv("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/emp.csv",columnNameOfCorruptRecord='broken')


# In[ ]:





# In[ ]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)


# In[ ]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show(truncate=False)


# In[ ]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,'right').show(truncate=False)


# In[ ]:


empDF.createOrReplaceTempView("EMP")


# In[ ]:


deptDF.createOrReplaceTempView("DEPT")


# In[ ]:


joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show(truncate=False)


# In[ ]:


joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show(truncate=False)


# In[ ]:


empDF.alias("table1").join(empDF.alias("table2"),     col("table1.superior_emp_id") == col("table2.emp_id"),"inner")     .select(col("table1.emp_id"),col("table1.name"),       col("table2.emp_id").alias("table2_emp_id"),       col("table2.name").alias("table2_emp_name"))    .show(truncate=False)


# In[ ]:




