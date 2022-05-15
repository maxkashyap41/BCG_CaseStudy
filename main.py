import os
import sys

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from src import welcome

if __name__ == '__main__':

    # from pyspark.sql import SparkSession

    # spark = SparkSession.builder .master("local").appName("App Name").getOrCreate()

    # welcome.say_hello('Radu')
     
    # print(spark.range(100000).where("id > 1000").selectExpr("sum(id)").collect())
