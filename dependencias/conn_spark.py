import findspark
from pyspark.sql import SparkSession

findspark.init("/home/jan/spark/spark-3.5.5-bin-hadoop3")

class CriarConn():
    def __init__(self, name_app):

        self.name_app = name_app

    def cria_conn(self):
        
        self.spark = SparkSession.builder \
            .appName(self.name_app) \
            .config("spark.driver.memory", "16g") \
            .config("spark.executor.memory", "16g") \
            .master("local[*]") \
            .getOrCreate()
        
        #display(self.spark)
        return self.spark