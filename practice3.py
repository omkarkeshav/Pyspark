#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


import pandas as pd


# In[3]:


from pyspark import SparkConf,SparkContext


# In[4]:


from pyspark.sql import SQLContext


# In[5]:


conf = (SparkConf().setMaster("local").setAppName("myapp"))


# In[6]:


sc = SparkContext(conf = conf)


# In[7]:


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]


# In[8]:


columns = ["firstname","middlename","lastname","dob","gender","salary"]


# In[9]:


df = SQLContext(sc)


# In[10]:


df= df.createDataFrame(data=data, schema = columns)


# In[11]:


df.show()


# In[12]:


numbers = [3,5,2,8,-34,-8,2,-4,-9,34,54]


# In[13]:


def cube(x):
    return x**3


# In[14]:


map(cube,numbers)


# In[15]:


for x in map(cube,numbers):
    print(x)


# In[16]:


list(map(cube,numbers))


# In[17]:


numbers


# In[18]:


list(map(lambda x:x*x,numbers))


# In[19]:


list(map(lambda x:x+x,numbers))


# In[20]:


marks = [[56,89,78],[56,90,78],[90,67,87]]


# In[21]:


len(marks)


# In[22]:


len(marks[0])


# In[23]:


list(map(sum,marks))


# In[24]:


list(map(lambda x:sum(x),marks))


# In[25]:


sum(list(map(lambda x:sum(x),marks)))


# In[26]:


# average of marks
list(map(lambda x:sum(x)/len(x),marks))


# In[27]:


list(map(lambda x:x[0],marks))


# In[28]:


friends = ["hari","ravi","lakshmi","ranga"]


# In[29]:


map(len,friends)


# In[30]:


list(map(len,friends))


# In[31]:


list(map(lambda x:x.upper(),friends))


# In[32]:


list(filter(lambda x:len(x)>4,friends))


# In[33]:


list(filter(lambda x:len(x)<5,friends))


# In[34]:


numbers = [3,5,2,8,-34,-8,2,-4,-9,34,54]


# In[35]:


list(filter(lambda x:x<0,numbers))


# In[36]:


from functools import reduce


# In[37]:


reduce(lambda x,y:x+y,numbers)


# reduce works like this format
# 3+5+2+8-34-8+2-4-9+34+54 53

# In[38]:


sum(numbers)


# In[39]:


reduce(lambda x,y:x*y,numbers)


# In[40]:


class Rectangle:
    def __init__(self,breadth,length):
        self.breadth = breadth
        self.length = length
    def getarea(self):
        return self.breadth * self.length


# In[41]:


j = Rectangle(4,5)


# In[42]:


k = Rectangle(56,90)


# In[43]:


k.length


# In[44]:


k.getarea()


# In[45]:


# pip install matplotlib


# In[47]:


import matplotlib.pyplot as plt


# In[48]:


class Circle:
    def __init__(self,radius,color):
        self.radius = radius
        self.color = color
    def getarea(self):
        return (22/7) * (self.radius**2)
    def draw(self):
        mycircle = plt.Circle((0.5,0.5),self.radius,color=self.color)
        plt.gcf().gca().add_artist(mycircle)


# In[49]:


a = Circle(0.1,'red')


# In[50]:


a.getarea()


# In[51]:


a.draw()


# In[ ]:




