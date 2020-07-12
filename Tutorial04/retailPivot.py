from pyspark import SparkContext
from pyspark.sql.session import SparkSession

import pyspark.sql.functions as f

def main():
    sc = SparkContext("local", "dataframe app")
    sc.setLogLevel("ERROR")
    spark = SparkSession(sc)

    #load the retail dataset:
    retail_data = spark.read.option("inferSchema", "true").option("header", "true").csv("/Users/faizan/Documents/Masters/2nd_Semester/Big_Data/Tutorial/Tutorials/Tutorial04/online-retail-dataset.csv")

    retail_data.show()

    result = retail_data.groupBy("StockCode").pivot("Country").sum("Quantity").orderBy("StockCode")
    result - result.na.fill(0)
    result.show()

    sc.stop()

if __name__ == "__main__":
    main()