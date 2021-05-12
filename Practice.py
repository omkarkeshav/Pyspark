#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def max_of_two(x,y):
    if x > y:
        return x
    return y

def max_of_three(x,y,z):
    return max_of_two(x,max_of_two(y,z))


# In[ ]:


max_of_three(4,6,8)


# In[ ]:


def multiply(numbers):
    total = 1
    for x in numbers:
        total *= x
    return total

multiply([1,2,3,4])


# In[ ]:


def caseCounter(s):
    d = {"UPPER_CASE":0,"LOWER_CASE":0}
    for c in s:
        if c.isupper():
            d['UPPER_CASE'] += 1
        elif c.lower():
            d['LOWER_CASE'] += 1
        else:
            pass
        
    print(f"No. of uppercase characters: {d['UPPER_CASE']}")
    print(f"No. of lowercase characters: {d['LOWER_CASE']}")
    
caseCounter("Omkar CHANDRAKANT keShav")


# In[ ]:


def duplicateEliminator(l):
    return set(l)

duplicateEliminator([1,1,2,2,3,3,4,4,5,5])


# In[ ]:


def squareMachine(theList):
    i = 0
    while i < len(theList):
        theList[i] = theList[i] **2
        i +=1
    return theList

squareMachine([1,1,2,3,4,5,6])


# In[ ]:


def dictDecomposer(theDict):
    print(f"The keys of the dictionary are {list(theDict.keys())}")
    print(f"The Values of the dictionary are {list(theDict.values())}")
testDictionary = {
    "Name" : "Omkar",
    "End" : "Keshav"
}
dictDecomposer(testDictionary)


# In[ ]:


class BasicClass:
    theVariable = 1


# In[ ]:


class ExampleClass:
    age = 24
    name = 'John Doe'
    location = 'Toronto, Ontario'


# In[ ]:


example_instance = ExampleClass()
print(example_instance.age)
print(example_instance.name)
print(example_instance.location)


# In[ ]:


class BirthdayBoy:
    def __init__(self,name,age):
        self.name = name
        self.age = age
    
    def birthday(self):
        self.age += 1
        
me = BirthdayBoy('Omkar',22)
print(me.age)
print(me.name)

me.birthday()
print(me.age)


# In[ ]:


class Salesperson:
    def __init__(self,firstName,lastName):
        self.firstName = firstName
        self.lastName = lastName
        
    sales = 0
    
    def makeSale(self,saleValue):
        self.sales += saleValue
        
    def salesReport(self):
        print(f"My total sales are {self.sales}!")


# In[ ]:


me = Salesperson('Omkar','Keshav')
me.makeSale(600)
me.salesReport()


# In[ ]:


candianProvinces = [
    'Ontario',
    'Quebec',
    'British Columbia'
    'New Brunswick',
    'Nova Scotia',
    'Price Edward Island',
    'Newfoundland and Labrador',
    'Saskatchewan',
    'Manitoba',
    'Alberta',
    'British Columbia'
]

for Provinces in candianProvinces:
    print(Provinces)


# In[ ]:


integers = [1,2,3,4,5]

for number in integers:
    print(number)
    print(number**2)


# In[ ]:


babyNames = ['Nick','Levi','Josiah','Micaiah','Sam','Taylor','Joel']

for name in babyNames:
    print(name)


# In[ ]:


carmakers = ['Toyota','Honda','Ford','GM']

for manafacturer in carmakers:
    print(manafacturer)


# In[ ]:


numbers = [1,2,3,4,5,6,7,8,9]

count_even = 0
count_odd = 0

for x in numbers:
    if(x%2 == 0):
        count_even += 1
    elif(x%2 == 1):
        count_odd += 1
    else:
        pass
print(f"The number of even number is : {count_even}")
print(f"The number of odd number is : {count_odd}")


# In[ ]:


integers = [2,4,5,6]

for number in integers:
    if(number % 2 == 0):
        print(number)


# printing Infinite Loop

# In[ ]:


# while(True):
#     print('This Course is Awesome')


# In[ ]:


number = int(input("enter a number"))

fact = 1

if (number == 0):
    print(1)
else:
    while number >= 1:
        fact = fact * number
        number -= 1
    print(f"The Factorial of number is {fact}")


# In[ ]:


furniture = ['table','chair','desk','lamp','couch']

for item in furniture:
    print(f"you will not be able to fina a {item.upper()} cheaper than our prices at furniture mart")


# In[ ]:


# while loop
furniture = ['table','chair','desk','lamp','couch']
i = 0

while i < len(furniture):
    print(f"you will not be able to find a {furniture[i].upper()} cheaper than our prices at furniture mart")
    i+= 1


# In[ ]:


integerList = [1,2,3,4,5,6]
i = 0
multiplierVariable = 1
while i < len(integerList):
    multiplierVariable = multiplierVariable * integerList[i]
    i += 1

print(f"The Product of all of the items in the list is {multiplierVariable}")
    


# In[ ]:


integerList = [1,2,3,4,5,6]

multiplierVariable = 1
for number in integerList:
    multiplierVariable = multiplierVariable * number
    
print(f"The Product of all of the items in the list is {multiplierVariable}")


# In[ ]:


firstMeal = 'breakfast'
secondMeal = 'lunch'
thirdMeal = 'supper'

firstMeal != secondMeal != thirdMeal


# In[ ]:


number = int(input("enter a number"))

if (number < 10):
    print("Number is less than 10")
elif(number >= 10 and number <= 20):
    print("Number is between 10 and 20,inclusive")
else:
    print("Number is greater than 20")
        


# In[ ]:


nameDictionary = {
    'firstName' : 'Nick',
    'lastName' : 'Mccullum'
}
print(nameDictionary)
type(nameDictionary)


# In[ ]:


carDict = {
    "brand" : "Ford",
    "model" : "Mustang",
    "year" : 1964
}

list(carDict.keys())
# type(list(carDict.keys()))


# In[ ]:


baseballTeams = {
    "Colorado" : "Rockies",
    "Boston" : "Red Sox",
    "Minnesota" : "Twins",
    "Milwaukee" : "Brewers",
    "seattle" : "Mariners"
}

list(baseballTeams.values())


# In[ ]:


ageDictionary = {"Trim":18,"Charlie":12,"Tiffany":22,"Robert":25}

ageDictionary.clear()

print(ageDictionary)


# In[ ]:


phonebook = {
    'Andrew Parson':8806336,
    'Emily Everett':6784346,
    'Peter Power' : 7658344,
    'Lewis Lame' : 1122345
}
phonebook.pop('Peter Power')
print(phonebook)


# In[ ]:


my_first_tuple = (2,5,4,9)
print(my_first_tuple)
type(my_first_tuple)


# In[ ]:


my_second_tuple = (123,456,(789,879))
print(my_second_tuple)


# In[ ]:


a_tuple = ('one',2,'three')
print(a_tuple[1])


# In[ ]:


quarterbacks = ('Tom Brady','Joe Montana','Michael Vick','Peyton Manning')

quarterbacks.index('Joe Montana')


# In[ ]:


runningLog = ('5 miles','3 miles','5 miles','10 miles','26.2 miles - marathon baby!')

runningLog.count('5 miles')


# In[ ]:




