#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init()


# In[ ]:


from pyspark import SparkConf,SparkContext


# In[ ]:


conf = (SparkConf().setMaster("local").setAppName("MyApp"))


# In[ ]:


sc = SparkContext(conf = conf)


# In[ ]:


myconf = SparkConf()


# In[ ]:


dir(myconf)


# In[ ]:


conf.getAll()


# In[ ]:


import math


# In[ ]:


n = sc.parallelize([10,5,8,20])


# In[ ]:


f = n.map(math.factorial)


# In[ ]:


factlist = f.collect()


# In[ ]:


for fact in factlist:
    print(fact)


# In[ ]:


fruits = sc.parallelize(['apple pear Litchi','grapes Melon'])


# In[ ]:


words = fruits.map(lambda f:f.split())


# In[ ]:


fruitslist = words.collect()


# In[ ]:


for fruit in fruitslist:
    print(fruit)


# In[ ]:


marks = sc.parallelize([[17,20,12],[13,16,19],[20,21,22]])


# In[ ]:


marks.map(lambda x:2*x).collect()


# In[ ]:


num = sc.parallelize([101,217,300,50,60,1000,180,2000])


# In[ ]:


tlist = num.filter(lambda n:n%2 ==1)


# In[ ]:


t = tlist.collect()


# In[ ]:


for x in t:
    print(x)


# In[ ]:


temperature = sc.parallelize([(1978,0),(1950,22),(1949,-11),(2019,30),(2000,78)])


# In[ ]:


tlist = temperature.filter(lambda x:x[1]>0).collect()


# In[ ]:


for x in tlist:
    print(f"{x[0]}-{x[1]}")
    print("{} - {}".format(x[0],x[1]))


# In[ ]:


data = sc.parallelize(range(1,100))


# In[ ]:


data.sample(False,.5).count()


# In[ ]:


data.sample(True,.1).count()


# In[ ]:


data.sample(False,.5).collect()


# In[ ]:


data.sample(True,.3).collect()


# In[ ]:


s = data.sample(False,0.5,20)


# In[ ]:


print(s)


# In[ ]:


s = data.sample(False,0.5)


# In[ ]:


s


# In[ ]:


one = sc.parallelize(range(1,10))


# In[ ]:


two = sc.parallelize(range(5,15))


# In[ ]:


print(one.count())


# In[ ]:


print(two.count())


# In[ ]:


print(one.union(two).collect())


# In[ ]:


print(one.union(two).count())


# In[ ]:


sample1 = one.sample(False,0.2,42)


# In[ ]:


sample2 = two.sample(False,0.2,42)


# In[ ]:


union_of_sample1_sample2 = sample1.union(sample2)


# In[ ]:


print(len(sample1.collect()),len(sample2.collect()),len(union_of_sample1_sample2.collect()))


# In[ ]:


one = sc.parallelize(range(1,10))


# In[ ]:


two = sc.parallelize(range(5,15))


# In[ ]:


one.intersection(two).collect()


# In[ ]:


data1 = sc.parallelize(range(1,19))


# In[ ]:


data2 = sc.parallelize(range(5,15))


# In[ ]:


data.union(data2).distinct().collect()


# In[ ]:


x = sc.parallelize([
    ("USA","CHICAGO"),
    ("USA","NEw YORK"),
    ("INDIA","NEW DELHI"),
    ("UK","LONDON"),
    ("INDIA","BANGLORE"),
    ("INDIA","MUMBAI"),
    ("USA","ATLANTA"),
    ("USA","BOSTON"),
    ("USA","CHICAGO"),
    ("INDIA","HYDERBAD"),
    ("UK","MANCHESTER"),
    ("USA","BRISTOL"),
],3)


# In[ ]:


y = x.groupByKey()


# In[ ]:


y.take(4)


# In[ ]:


for t in y.collect():
    print(t[0],[x for x in t[1]])


# In[ ]:


for t in y.collect():
    print(t[0],list(t[1]))


# In[ ]:


rdd = sc.parallelize(range(100))


# In[ ]:


rdd.reduce(lambda a,b:a+b)


# In[ ]:


rdd.reduceByKey(lambda a,b:a+b)


# In[ ]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


# In[ ]:


data = [('Project', 1),
('Gutenberg’s', 1),
('Alice’s', 1),
('Adventures', 1),
('in', 1),
('Wonderland', 1),
('Project', 1),
('Gutenberg’s', 1),
('Adventures', 1),
('in', 1),
('Wonderland', 1),
('Project', 1),
('Gutenberg’s', 1)]


# In[ ]:


rdd=spark.sparkContext.parallelize(data)


# In[ ]:


rdd2=rdd.reduceByKey(lambda a,b: a+b)
for element in rdd2.collect():
    print(element)


# In[ ]:


x = sc.parallelize([
    ("mangoes",1),
    ("grapes",1),
    ("mangoes",1),
    ("grapes",1),
    ("grapes",1),
    ("grapes",15),
    ("grapes",1),
    ("mangoes",1),
    ("apple",10)
])


# In[ ]:


y = x.reduceByKey(lambda accum,n:accum +n)


# In[ ]:


for element in y.collect():
    print(element)


# In[ ]:


def sumFunc(accum,n):
    return accum+n


# In[ ]:


y = x.reduceByKey(sumFunc)
print(y.collect())


# In[ ]:


fr = sc.parallelize([
    ("mangoes",11),
    ("grapes",10),
    ("apples",16),
    ("pears",18),
    ("melon",20),
    ("kiwi",150),
    ("Iitchi",90),
    ("banans",1)
])


# In[ ]:


y = fr.sortByKey()


# In[ ]:


for x in y.collect():
    print(x)


# In[ ]:


fr.sortByKey(False).collect()


# In[ ]:


fruits = sc.parallelize(["mango","mango","mango","mango","pear","pear","pear","pear","apple"]).map(lambda x:(x,1))


# In[ ]:


fruits.take(2)


# In[ ]:


fruits.reduceByKey(lambda a,b:a+b).collect()


# In[ ]:


names1 = sc.parallelize(('ram','sita','laxman')).map(lambda a:(a,1))


# In[ ]:


names1.take(2)


# In[ ]:


names2 = sc.parallelize(('sita','ram','sriya','kriya')).map(lambda a:(a,2))


# In[ ]:


names2.take(1)


# In[ ]:


names1.join(names2).collect()


# In[ ]:


names1.leftOuterJoin(names2).collect()


# In[ ]:


names1.rightOuterJoin(names2).collect()


# In[ ]:


# fruits1.map(lambda x:(x[1],x[0])).collect()


# In[ ]:


a = sc.parallelize([(1,2),(2,3),(5,4)])


# In[ ]:


b = sc.parallelize([(1,0),(2,5),(5,5)])


# In[ ]:


d = a.cogroup(b)


# In[ ]:


d.take(3)


# In[ ]:


d.map(lambda x:(x[0],list(x[1][0]),list(x[1][1]))).collect()


# In[ ]:


a = sc.parallelize([(1,2),(2,3),(5,4),(6,9)])


# In[ ]:


b = sc.parallelize([(1,0),(2,5),(5,5),(7,3)])


# In[ ]:


d = a.cogroup(b)


# In[ ]:


d.take(3)


# In[ ]:


d.map(lambda x:(x[0],list(x[1][0]),list(x[1][1]))).collect()


# In[ ]:


a = sc.parallelize(range(1,5))
b = sc.parallelize(range(10,15))


# In[ ]:


a.cartesian(b).collect()


# In[ ]:


sc.parallelize(range(1,20),5).glom().collect()


# In[ ]:


sc.parallelize(range(1,21),5).glom().collect()


# In[ ]:


sc.parallelize(range(1,21),5).coalesce(3).glom().collect()


# In[ ]:


rdd1 = sc.parallelize(range(1,21),5)


# In[ ]:


rdd1.glom().collect()


# In[ ]:


rdd1.coalesce(3).glom().collect()


# In[ ]:


# repartition(numPartitions)


# In[ ]:


rdd = sc.parallelize([1,2,3,4,5,6,7,],4)


# In[ ]:


rdd.glom().collect()


# In[ ]:


(rdd.repartition(2).glom().collect())


# In[ ]:


distFile = sc.textFile("sample.txt")


# In[ ]:


sc.textFile("sample.txt").count()


# In[ ]:


distFile.map(lambda x:x.upper()).collect()


# In[ ]:


distFile.map(lambda x:x.split()).collect()


# In[ ]:


distFile.flatMap(lambda x:x.split()).collect()


# In[ ]:


distFile.flatMap(lambda x:x.split()).distinct().collect()


# In[ ]:


len(distFile.flatMap(lambda x:x.split()).distinct().collect())


# In[ ]:


distFile.flatMap(lambda x:x.split()).distinct().count()


# In[ ]:


# lazy execution


# In[ ]:


lines = sc.textFile("sample.txt")


# In[ ]:


lineLengths = lines.map(len)


# In[ ]:


print(lineLengths.take(2))


# In[ ]:


print(lineLengths.count())


# In[ ]:


totalLength = lineLengths.reduce(lambda a,b:a+b)
print(totalLength)


# In[ ]:


# working with key value data


# In[ ]:


lines = sc.textFile("sample.txt")


# In[ ]:


words = lines.flatMap(lambda line:line.split())


# In[ ]:


pairs = words.map(lambda s:(s,1))


# In[ ]:


print(pairs.take(6))


# In[ ]:


counts = pairs.reduceByKey(lambda a,b:a+b)
print(counts.collect())


# In[ ]:


# generators


# In[ ]:


squares = (x*x for x in range(1,11))


# In[ ]:


type(squares)


# In[ ]:


print(squares)


# In[ ]:


for number in squares:
    print(number)


# In[ ]:


def countdown(x):
    while x>0:
        print(x)
        x = x-1


# In[ ]:


countdown(5)


# In[ ]:


def countdown1(x):
    while x > 0:
        yield x
        x = x-1


# In[ ]:


countdown1(5)


# In[ ]:


for x in countdown1(5):
    print(x)


# In[ ]:


parallel = sc.parallelize(range(1,10),3)


# In[ ]:


parallel.glom().collect()


# In[ ]:


parallel.map(lambda x:x*x).collect()


# In[ ]:


# parallel.mapPartitions(lambda x: x*x ).collect()


# In[ ]:


def f(iterator):
    yield sum(iterator)


# In[ ]:


parallel.mapPartitions(f).collect()


# In[ ]:


parallel = sc.parallelize(range(1,10),3)


# In[ ]:


parallel.glom().collect()


# In[ ]:


def show(index,iterator):
    yield f"index : {index} values :{list(iterator)}"


# In[ ]:


parallel.mapPartitionsWithIndex(show).collect()


# In[ ]:


def show(index,iterator):
    yield f"index : {index} values:{sum(iterator)}"


# In[ ]:


parallel.mapPartitionsWithIndex(show).collect()


# In[ ]:


#unpacking


# In[ ]:


numbers = [2,3,4]


# In[ ]:


x,y,z = numbers


# In[ ]:


#printing Dictionaries


# In[ ]:


stdcode = {"hyd": 40,"blr":800,"del":11,"mum":22}


# In[ ]:


print(stdcode)


# In[ ]:


stdcode.keys()


# In[ ]:


stdcode.values()


# In[ ]:


stdcode.items()


# In[ ]:


for x in stdcode.items():
    print(x)


# In[ ]:


for x,y in stdcode.items():
    print(x)


# In[ ]:


for city,std in stdcode.items():
    print(city,std)


# In[ ]:


for city,std in stdcode.items():
    print(f"city = {city} stdcode = {std}")


# In[ ]:


#lookup a dict


# In[ ]:


stdcode['blr']


# In[ ]:


cities = ("hyd","blr")


# In[ ]:


stdcodes = ("040","080")


# In[ ]:


zip(cities,stdcodes)


# In[ ]:


for x in zip (cities,stdcodes):
    print(x)


# In[ ]:




