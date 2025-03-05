import findspark
from pyspark.sql import SparkSession

findspark.init("/home/jan/spark/spark-3.5.5-bin-hadoop3")

def cria_conn(name_app):
    
    spark = SparkSession.builder \
        .appName(name_app) \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "16g") \
        .master("local[*]") \
        .getOrCreate()
    
    #display(self.spark)
    return spark