from pyspark import SparkContext, SQLContext
from pyspark.sql import functions as F, Row
import sys

if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    def parseCSV(indx, part):
        import csv, sys
        if indx == 0:
            if (sys.version_info > (3, 0)): # check python version
                next(part)
            else:
                part.next()
        for p in csv.reader(part):
            yield Row(product=p[1],date_recieved=p[0], company=p[7])
        
    rows = sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'complaints_small.csv')\
        .mapPartitionsWithIndex(parseCSV).cache()
    
    df = sqlContext.createDataFrame(rows)

    df_prodyearcomp = df.select(F.upper("product").alias("product"), F.year("date_recieved").alias("year"), F.col("Company"))\
        .groupBy(F.col("product"), F.col("year"), F.col("Company")).count()
        
    # df_prodyearcomp.show()
    
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
