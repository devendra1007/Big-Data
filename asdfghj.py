# Exercise 1

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Exercise1').getOrCreate()

foodratings_schema = StructType([
  StructField('name', StringType(), True),
  StructField('food1', IntegerType(), True),  
  StructField('food2', IntegerType(), True),
  StructField('food3', IntegerType(), True),
  StructField('food4', IntegerType(), True),
  StructField('placeid', IntegerType(), True)  
])

foodratings = spark.read.csv('foodratings180652.txt', schema=foodratings_schema)

# Print schema and show 5 records
print(foodratings.printSchema()) 
foodratings.show(5)

# Exercise 2

foodplaces_schema = StructType([
  StructField('placeid', IntegerType(), True),
  StructField('placename', StringType(), True)
])

foodplaces = spark.read.csv('foodplaces.txt', schema=foodplaces_schema)

print(foodplaces.printSchema())
foodplaces.show(5)

# Exercise 3

foodratings.createOrReplaceTempView('foodratingsT')
foodplaces.createOrReplaceTempView('foodplacesT')

foodratings_ex3a = spark.sql("""
  SELECT * FROM foodratingsT 
  WHERE food2 < 25 AND food4 > 40
""")

print(foodratings_ex3a.printSchema())
foodratings_ex3a.show(5)

foodplaces_ex3b = spark.sql("""
  SELECT * FROM foodplacesT
  WHERE placeid > 3  
""")

print(foodplaces_ex3b.printSchema())  
foodplaces_ex3b.show(5)

# Exercise 4

foodratings_ex4 = foodratings.where("name = 'Mel' AND food3 < 25")

print(foodratings_ex4.printSchema())
foodratings_ex4.show(5)

# Exercise 5 

foodratings_ex5 = foodratings.select('name', 'placeid')

print(foodratings_ex5.printSchema())
foodratings_ex5.show(5)

# Exercise 6

ex6 = foodratings.join(foodplaces, foodratings.placeid == foodplaces.placeid, 'inner') 

print(ex6.printSchema())
ex6.show(5)