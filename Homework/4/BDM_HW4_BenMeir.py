from pyspark import SparkContext, SQLContext
from pyspark.sql import functions as F, Row
import sys


def py3_solution():
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
    
    df_report.toPandas().to_csv("report.csv")
    # df_report.show()
    df_report.write.csv(sys.argv[2] if len(sys.argv)>2 else 'report', header=True)

def py2_solution():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    def parseCSV(indx, part):
        import csv
        if indx == 0:
            part.next()
        for p in csv.reader(part):
            year = datetime.datetime.strptime(row[0], '%Y-%m-%d').year
            yield Row(product=p[1].upper(), year=year, company=p[7])
    
    def aggr_row_data(x):
        complaints = sum(x[1])
        return Row(
            product=x[0][0].upper(),
            year=x[0][1],
            companies = len(x[1]),
            complaints=complaints,
            highest_percent = round(100*max(x[1])/complaints)
        )

    rows = sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'complaints_small.csv')\
        .mapPartitionsWithIndex(parseCSV).cache()
    
    df = sqlContext.createDataFrame(rows)
    df1 = df.select(F.col("product"), F.col("year"), F.col("Company"))\
        .groupBy(F.col("product"), F.col("year"), F.col("Company")).count().cache()
    rdd1 = df.rdd.map(lambda x: ((x.product, x.year), (x.count, 1))).groupByKey()\
        .map(aggr_row_data).cache()
    df2 = sqlContext.createDataFrame(rdd2)

    df2.toPandas().to_csv("report.csv")
    # df2.show()
    df2.write.csv(sys.argv[2] if len(sys.argv)>2 else 'report', header=True)

if __name__ == '__main__':
    if sys.version_info[0] == 3:
        py3_solution()
    else:
        py2_solution()
