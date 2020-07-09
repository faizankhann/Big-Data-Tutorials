from pyspark import SparkContext
from pyspark.sql.session import SparkSession

import pyspark.sql.functions as f


def main():
    sc = SparkContext("local", "dataframe app")
    sc.setLogLevel("ERROR")
    spark = SparkSession(sc)

    #load the retail dataset
    retail_data = spark.read.option("inferSchema", "true").option("header", "true").option("timestampFormat", "dd/M/yyyy H:mm").csv("/Users/faizan/Documents/Masters/2nd_Semester/Big_Data/Tutorial/Tutorials/Tutorial02/online-retail-dataset.csv")
    retail_data.show()

    #Question 1
    #How many orders did customers perform at which hour?

    # a) SQL
    retail_data.createOrReplaceTempView("retailTable")

    result = spark.sql("""
    SELECT hour(InvoiceDate) as InvoiceHour, count(distinct InvoiceNo) as NoInvoices
    FROM retailTable
    GROUP BY InvoiceHour
    ORDER BY InvoiceHour
    """)
    result.show()

    # b) Spark
    result = retail_data.selectExpr("hour(InvoiceDate) as InvoiceHour", "InvoiceNo").distinct().groupBy("InvoiceHour").agg(f.expr("count(InvoiceNo) as NoInvoices")).orderBy("InvoiceHour")
    result.show()

    #Question 2
    #How frequently was each product bought in the different countries?

    # a) SQL
    df_selection = retail_data.selectExpr("Country", "StockCode", "Quantity")
    df_nonull = df_selection.na.replace([""],["UNKNOWN"], "StockCode").na.replace([""],["UNKNOWN"], "Country").na.drop("any")
    df_nonull.createOrReplaceTempView("retailNoNull")

    result = spark.sql("""
    SELECT Country, StockCode, sum(Quantity) as Quantity
    FROM retailNoNull
    GROUP BY Country, StockCode
    GROUPING SETS ((Country, StockCode), (Country), (StockCode), ())
    ORDER BY Country, StockCode
    """)
    result.show()

    # b) Spark
    result = df_nonull.cube("Country", "StockCode").agg(f.sum("Quantity").alias("Quantity")).orderBy(f.col("Country"), f.col("StockCode"))
    result.show()

    result.coalesce(1).write.format("csv").option("header", "true").save("/Users/faizan/Documents/Masters/2nd_Semester/Big_Data/Tutorial/Tutorials/Tutorial03/frequencies")
    sc.stop()

if __name__ == "__main__":
    main()