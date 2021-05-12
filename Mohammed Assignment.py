#!/usr/bin/env python
# coding: utf-8

# In[2]:


import findspark
findspark.init()


# In[3]:


from pyspark.sql.functions import col


# In[4]:


from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import monotonically_increasing_id
# from pyspark.sql.functions import col,lit
from pyspark.sql.window import Window as W
# from pyspark.sql import functions as F
from pyspark.sql.window import Window


# In[5]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkExamples").getOrCreate()


# In[6]:


empdf = spark.read.format("csv").options(header=True,inferschema = True).load("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/emp.csv")


# In[7]:


empdf.show()


# In[8]:


deptdf = spark.read.format("csv").options(header='true', inferschema='true',truncate='true').load("C:/Users/ochandrakantkesha/Desktop/Vanguard Project/Assignment/dept.csv")


# In[9]:


deptdf.show(truncate =False)


# In[10]:


# df.withColumn("name",col("name").cast("String")).show()


# In[10]:


empdf.createOrReplaceTempView("EMP")


# In[11]:


deptdf.createOrReplaceTempView("DEPT")


# In[14]:


joinDF = spark.sql("select row_number() over(order by emp_id) as sr_id ,emp_id,name,salary,dept_name,dept_id from EMP e, DEPT d  where e.emp_dept_id == d.dept_id")


# In[15]:


joinDF.show(truncate=False)


# OR

# In[16]:


df1 = joinDF.withColumn("idx", monotonically_increasing_id())
windowSpec = W.orderBy("idx")


# In[17]:


df2 = df1.withColumn("idx",row_number().over(windowSpec))
df2.show(truncate=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[9]:





# In[10]:





# In[ ]:





# In[ ]:





# In[18]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




