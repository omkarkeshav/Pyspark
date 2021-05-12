#!/usr/bin/env python
# coding: utf-8

# using map find out the number of characters in each line of the file

# In[ ]:


f = open("new1.txt","r")


# In[ ]:


lines = f.readlines()


# In[ ]:


for index,line in enumerate(lines):
    print("Line number {} has {} characters".format(index+1,len(line)))


# In[ ]:


f.close()


# doing it with map

# In[ ]:


f = open("new1.txt","r")
# lines = f.readlines()
for x in map(len,f):
    print(x)
f.close()


# In[ ]:


f = open("new1.txt","r")
print(list(map(len,f)))
f.close()


# using map print the number of words in a line

# In[ ]:


f = open("new1.txt","r")
lines = f.readlines()
for x in map(lambda line:len(line.split()),lines):
    print(x)
f.close()


# using map print the number of words and characters in a line

# In[ ]:


f = open("new1.txt")
lines = f.readlines()
print(list(map(lambda line:(len(line.split()),len(line)),lines)))
f.close()


# In[ ]:


f = open("new1.txt")
lines = f.readlines()
print(list(map(lambda line:[len(line.split()),len(line)],lines)))
f.close()


# In[ ]:


f = open("new1.txt","r")
lines = f.readlines()
print(list(map(lambda line:[len(line.split()),len(line),line.upper().strip()],lines)))
f.close()


# Trying Map-Reduce-Filter-

# In[ ]:


marks = [("ravi",34,90,76),("range",78,90,76),("hari",67,91,56),("adi",30,90,76)]


# print the total marks of students who passed in all subjects

# filter - eliminate failed
# map - get name and marks separately
# reduce - sum

# In[ ]:


list(filter(lambda x:True,marks))


# In[ ]:


list(filter(lambda x:False,marks))


# In[ ]:


list(filter(lambda x:x[1]>34,marks))


# In[ ]:


list(map(lambda x:x[1:],marks))


# In[ ]:


list(map(lambda x:min(x[1:]),marks))


# In[ ]:


list(map(lambda x:min(x[1:])>34,marks))


# In[ ]:


list(filter(lambda x:min(x[1:])>34,marks))


# In[ ]:


marks = [("ravi",34,90,76),("range",78,90,76),("hari",67,91,56),("adi",80,30,76)]


# In[ ]:


list(filter(lambda x:min(x[1:])>34,marks))


# In[ ]:


# filter(lambda x:min(x[1:])>34,marks))
# map(lambda x:(x[0],sum(x[1:])),
list(map(lambda x:(x[0],sum(x[1:])),filter(lambda x:min(x[1:])>34,marks)))


# when we want to use reduce import functools

# In[ ]:


from functools import reduce


# In[ ]:


# list(map(lambda x:(x[0],sum(x[1:])),filter(lambda x:min(x[1:])>34,marks)))

reduce(lambda a,b:a[1]+b[1],list(map(lambda x:(x[0],sum(x[1:])),filter(lambda x:min(x[1:])>34,marks))))


# In[ ]:


marks = [("ravi",34,90,76),("range",78,90,76),("hari",67,91,56),("adi",80,39,76)]


# In[ ]:


list(map(lambda x:sum(x[1:]),filter(lambda x:min(x[1:])>34,marks)))


# In[ ]:


reduce(lambda a,b:a+b,map(lambda x:sum(x[1:]),filter(lambda x:min(x[1:])>34,marks)))


# In[ ]:


# help("modules data")


# In[ ]:


# help("modules")


# In[ ]:


import os 
import sys


# In[ ]:


sys.version


# In[ ]:


os.environ


# In[ ]:


sys.path


# In[ ]:


sys.platform


# In[ ]:


pip show bottle


# In[ ]:


numbers = [2,5,3,-4,-5,34,56,78]


# In[ ]:


squares = []


# In[ ]:


for x in numbers:
    squares.append(x*x)


# In[ ]:


squares


# In[ ]:


[x*x for x in numbers]


# In[ ]:


[x*x for x in range(1,11)]


# In[ ]:


mysquares = [x*x for x in range(1,11)]


# In[ ]:


mysquares


# In[ ]:


import random


# In[ ]:


[random.random() for x in range(10)]


# In[ ]:


[random.randint(1,100) for x in range(20)]


# In[ ]:


[x*x for x in numbers if x%2==0]


# In[ ]:


r = [random.randint(1,100) for x in range(20)]


# In[ ]:


for x in r:
    print(x)


# In[ ]:


j = [random.randint(1,100) for x in range(200000)]


# In[ ]:


import sys


# In[ ]:


sys.getsizeof(j)/1024/1024


# In[ ]:


k = (random.randint(1,100) for x in range(200000))


# In[ ]:


sys.getsizeof(k)


# In[ ]:


for x in j:
    print(x)


# In[ ]:


type(j)


# In[ ]:


type(k)


# In[ ]:


map(abs,numbers)


# In[ ]:


for x in map(abs,numbers):
    print(x)


# In[ ]:


f = open("new1.txt","r")
k = [word for word in f.read().split()]
f.close()


# In[ ]:


k


# In[ ]:


f = open("new1.txt","r")
l =(word for word in f.read().split())
f.close()


# In[ ]:


type(l)


# In[ ]:


for x in l:
    print(x)


# In[ ]:


f = open("new1.txt","r")
type(f)


# In[ ]:


f.mode


# In[ ]:


f.name


# In[ ]:


f.closed


# In[ ]:


dir(f)


# In[ ]:


# help(f.closed)


# In[ ]:


a = [4,6,3,-3,-5,-6,9,6,7,8]


# In[ ]:


map(abs,a)


# In[ ]:




