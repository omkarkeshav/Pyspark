#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql.functions import col


# In[3]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkExamples").getOrCreate()


# In[4]:


emp = [(1,"Smith",-1,"2018","10","M",3000),     (2,"Rose",1,"2010","20","M",4000),     (3,"Williams",1,"2010","10","M",1000),     (4,"Jones",2,"2005","10","F",2000),     (5,"Brown",2,"2010","40","",-1),       (6,"Brown",2,"2010","50","",-1)   ]


# In[5]:


empColumns = ["emp_id","name","superior_emp_id","year_joined",        "emp_dept_id","gender","salary"]


# In[6]:


empDF = spark.createDataFrame(data=emp, schema = empColumns)


# In[7]:


empDF.printSchema()


# In[8]:


empDF.show(truncate=False)


# In[9]:


dept = [("Finance",10),     ("Marketing",20),     ("Sales",30),     ("IT",40)   ]


# In[10]:


deptColumns = ["dept_name","dept_id"]


# In[11]:


deptDF = spark.createDataFrame(data=dept, schema = deptColumns)


# In[12]:


deptDF.printSchema()


# In[13]:


deptDF.show(truncate=False)


# In[14]:


empDF.show(truncate=False)


# In[14]:


empDF.createOrReplaceTempView("EMP")


# In[15]:


deptDF.createOrReplaceTempView("DEPT")


# In[16]:


joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show(truncate=False)


# In[17]:


joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show(truncate=False)


# In[18]:


empDF.alias("table1").join(empDF.alias("table2"),     col("table1.superior_emp_id") == col("table2.emp_id"),"inner")     .select(col("table1.emp_id"),col("table1.name"),       col("table2.emp_id").alias("table2_emp_id"),       col("table2.name").alias("table2_emp_name"))    .show(truncate=False)


# In[19]:


empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id,'inner').show(truncate=False)


# In[20]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,'left').show(truncate = False)


# In[21]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,'right').show(truncate=False)


# In[22]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,'outer').show(truncate=False)


# In[23]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full").show(truncate=False)


# In[24]:


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show(truncate=False)

