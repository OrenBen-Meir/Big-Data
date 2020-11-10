from pyspark import SparkContext, SQLContext
from pyspark.sql import functions as F, Row
import sys

if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    df = sqlContext.read.csv(sys.argv[1] if len(sys.argv)>1 else 'complaints_small.csv', multiLine=True, \
        header=True, escape="\"", inferSchema=True, lineSep="\n")

    # print(df.columns)
    
    df_prodyearcomp = df.select(F.upper("Product").alias("product"), F.year("Date received").alias("year"), F.col("Company"))\
        .groupBy(F.col("product"), F.col("year"), F.col("Company")).count()
            
    df_report = df_prodyearcomp.groupBy(F.col("product"), F.col("year"))\
        .agg(F.sum("count").alias("Number of complaints"),\
            F.count("count").alias("Number of companies"),\
            F.max("count").alias("highest complaints"))\
        .orderBy("product", "year")\
        .withColumn("highest percentage", F.round(100*F.col("highest complaints")/F.col("Number of complaints")))\
        .drop(F.col("highest complaints"))
    
    # df_report.toPandas().to_csv("report.csv")
    # df_report.show()
    df_report.write.csv(sys.argv[2] if len(sys.argv)>2 else 'report', header=True)
