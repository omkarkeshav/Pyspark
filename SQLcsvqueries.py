#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init()


# In[9]:


from pyspark.sql.functions import col


# In[2]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkExamples").getOrCreate()


# In[3]:


empDF = spark.read.format("csv").options(header='true', inferschema='true',truncate='true').load("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/emp.csv")


# In[4]:


empDF.printSchema()


# In[5]:


deptDF = spark.read.format("csv").options(header='true', inferschema='true',truncate='true').load("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/dept.csv")


# In[6]:


deptDF.printSchema()


# In[7]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)


# In[8]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show(truncate=False)


# In[10]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,'right').show(truncate=False)


# In[11]:


empDF.createOrReplaceTempView("EMP")


# In[12]:


deptDF.createOrReplaceTempView("DEPT")


# In[13]:


joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show(truncate=False)


# In[14]:


joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show(truncate=False)


# In[15]:


empDF.alias("table1").join(empDF.alias("table2"),     col("table1.superior_emp_id") == col("table2.emp_id"),"inner")     .select(col("table1.emp_id"),col("table1.name"),       col("table2.emp_id").alias("table2_emp_id"),       col("table2.name").alias("table2_emp_name"))    .show(truncate=False)


# In[ ]:




