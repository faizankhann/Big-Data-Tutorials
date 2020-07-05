from pyspark import SparkContext

sc = SparkContext("local", "hello World")
sc.setLogLevel("ERROR")

print("Hello World")