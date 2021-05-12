#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql import SQLContext
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col
from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[3]:


spark = SparkSession.builder.master("local[1]").appName("SparkExamples").getOrCreate()


# In[4]:


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]


# In[5]:


columns = ["firstname","middlename","lastname","dob","gender","salary"]


# In[6]:


df = spark.createDataFrame(data=data, schema = columns)


# In[7]:


df.show()


# In[8]:


df.withColumn("salary",col("salary").cast("Integer")).show()


# In[9]:


df.withColumn("salary",col("salary")*100).show()


# In[10]:


df.withColumn("CopiedCol",col("salary")* -1).show()


# In[11]:


data = [("111",50000),("222",60000),("333",40000)]


# In[12]:


columns= ["EmpId","Salary"]


# In[13]:


df = spark.createDataFrame(data = data ,schema = columns)


# In[14]:


df.show()


# In[15]:


df1 = df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value1"))


# In[21]:


df1.show


# In[22]:


df1.show(truncate = False)


# In[23]:


# df2 = df1.withColumn("lit_value2", when(col("Salary") >=40000 & col("Salary") <= 50000,lit("100")).otherwise(lit("200")))


# In[24]:


# df2.show(truncate = False)


# Filter Condition

# In[25]:


data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]


# In[26]:


schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])


# In[27]:


df = spark.createDataFrame(data = data ,schema = schema)


# In[28]:


df.printSchema()


# In[29]:


df.show(truncate=False)


# In[30]:


df.filter(df.state == "OH").show(truncate=False)


# In[31]:


df.filter(df.state != "OH")     .show(truncate=False) 


# In[32]:


df.filter(col("state") == "OH")     .show(truncate=False)


# In[33]:


df.filter("gender == 'M'").show()


# In[34]:


df.filter("gender != 'M'").show()


# In[35]:


df.filter( (df.state  == "OH") & (df.gender  == "M") )     .show(truncate=False) 


# In[36]:


df.show(truncate=False)


# In[37]:


li=["OH","CA","DE"]
# Filter IS IN List values


# In[38]:


df.filter(df.state.isin(li)).show()


# In[39]:


# Filter NOT IS IN List values
df.filter(~df.state.isin(li)).show()


# In[40]:


df.filter(df.state.isin(li)==False).show()


# In[41]:


df.filter(df.state.startswith("N")).show()


# In[42]:


df.filter(df.state.endswith("H")).show()


# In[43]:


df.filter(df.state.contains("H")).show()


# In[44]:


df.filter(array_contains(df.languages,"Java"))     .show(truncate=False)


# In[45]:


df.filter(df.name.lastname == "Williams")     .show(truncate=False) 


# In[46]:


data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")]


# In[47]:


df1 = spark.createDataFrame(data = data2, schema = ["id","name"])


# In[48]:


df1.printSchema()


# In[49]:


df1.filter(df1.name.like("%rose%")).show()


# In[50]:


df1.filter(df1.name.rlike("(?i)^*rose$")).show()


# PySpark – Distinct to Drop Duplicate Rows

# In[51]:


data = [("James", "Sales", 3000),     ("Michael", "Sales", 4600),     ("Robert", "Sales", 4100),     ("Maria", "Finance", 3000),     ("James", "Sales", 3000),     ("Scott", "Finance", 3300),     ("Jen", "Finance", 3900),     ("Jeff", "Marketing", 3000),     ("Kumar", "Marketing", 2000),     ("Saif", "Sales", 4100)   ]


# In[52]:


columns= ["employee_name", "department", "salary"]


# In[53]:


df = spark.createDataFrame(data = data ,schema = columns)


# In[54]:


df.show(truncate=False)


# In[55]:


distinctDF = df.distinct()
print("Distinct Count :" +str(distinctDF.count()))
distinctDF.show(truncate = False)


# In[56]:


df2 = df.dropDuplicates()
print("Distinct count :" +str(df2.count()))
df2.show(truncate = False)


# In[57]:


dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department & salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)


# PySpark orderBy() and sort() explained

# In[58]:


simpleData = [("James","Sales","NY",90000,34,10000),     ("Michael","Sales","NY",86000,56,20000),     ("Robert","Sales","CA",81000,30,23000),     ("Maria","Finance","CA",90000,24,23000),     ("Raman","Finance","CA",99000,40,24000),     ("Scott","Finance","NY",83000,36,19000),     ("Jen","Finance","NY",79000,53,15000),     ("Jeff","Marketing","CA",80000,25,18000),     ("Kumar","Marketing","NY",91000,50,21000)   ]


# In[59]:


columns= ["employee_name","department","state","salary","age","bonus"]


# In[60]:


df = spark.createDataFrame(data = simpleData,schema = columns)


# In[61]:


df.printSchema()


# In[85]:


df.show(truncate=False)


# In[86]:


df.sort("department","state").show(truncate=False)


# In[87]:


df.sort(col("department"),col("state")).show(truncate=False)


# In[88]:


df.orderBy("department","state").show(truncate=False)


# In[89]:


df.orderBy(col("department"),col("state")).show(truncate=False)


# In[90]:


df.sort(df.department.asc(),df.state.asc()).show(truncate=False)


# In[91]:


df.sort(col("department").asc(),col("state").asc()).show(truncate=False)


# In[92]:


df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)


# In[93]:


df.sort(df.department.asc(),df.state.desc()).show(truncate=False)


# In[94]:


df.sort(col("department").asc(),col("state").desc()).show(truncate=False)


# In[95]:


df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)


# In[ ]:




