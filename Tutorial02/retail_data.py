from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import desc, sum, col 

def main():
    sc = SparkContext("local","dataframe app")
    sc.setLogLevel("ERROR")
    spark  = SparkSession(sc)

    #load retail dataset
    retail_data = spark.read.option("inferSchema", "true").option("header","true").csv("/Users/faizan/Documents/Masters/2nd_Semester/Big_Data/Tutorial/Tutorials/Tutorial02/online-retail-dataset.csv")
    
    retail_data.show()

    #Which item was bought most(total)?
    best_selling_items = retail_data.groupBy(col("StockCode")).agg(sum(col("Quantity")).alias("sum_quantity")).sort(desc("sum_quantity"))
    best_selling_items.show(5)
    retail_data.select("StockCode", "Description").where(col("StockCode") == "22197").dropDuplicates().show()

    #Which one was bought most in the USA?
    best_selling_items_usa = retail_data.where(col("Country") == "USA").groupBy(col("StockCode")).agg(sum(col("Quantity")).alias("sum_quantity")).sort(desc("sum_quantity"))
    best_selling_items_usa.show(5)
    retail_data.select("StockCode", "Description").where(col("StockCode") == "23366").dropDuplicates().show()
    
    #Which was the lowest invoice (>0), which one the highest?
    all_invoices = retail_data.groupBy(col("InvoiceNo")).agg(sum(col("UnitPrice")*col("Quantity")).alias("total_cost")).sort(col("total_cost"))
    all_invoices.show()

    positive_invoices = all_invoices.where(col("total_cost") > 0)
    positive_invoices.show()

    #Add a column which displays whether an item was purchased in Germany
    retail_data_germany = retail_data.withColumn("bought_in_germany", col("Country") == "Germany")
    retail_data_germany.show()

    #Add a colun which shows the total amount of the correspinding invoice.
    retail_data.join(all_invoices, ["InvoiceNo"]).show()

    #How many Germany customers spent more than $10?
    invoices_germany = retail_data.where(col("Country") == "Germany").groupBy(col("InvoiceNo")).agg(sum(col("UnitPrice")*col("Quantity")).alias("total_cost")).sort(col("total_cost"))
    invoices_germany.show()
    more_than_10 = invoices_germany.where(col("total_cost") > 10).count()
    print(more_than_10)

    #Sort the German customers with respect to their total invoice in descending order.
    german_customers = retail_data.where(col("Country") == "Germany").groupBy(col("InvoiceNo"), col("CustomerID")).agg(sum(col("UnitPrice")*col("Quantity")).alias("total_cost")).sort(desc("total_cost"))
    german_customers.show()
    
    sc.stop()

if __name__ == "__main__":
    main()
