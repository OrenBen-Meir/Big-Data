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
    
    # df_report.toPan/das().to_csv("report.csv")
    # df_report.show()
    df_report.write.csv(sys.argv[2] if len(sys.argv)>2 else 'report', header=True)

def py2_solution():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    # def parseCSV(indx, part):
    #     import csv, datetime
    #     if indx == 0:
    #         part.next()
    #     for p in csv.reader(part):
    #         year = datetime.datetime.strptime(p[0], '%Y-%m-%d').year
    #         product, company= p[1], p[7]
    #         yield (product, year, company), 1
    
    def map_row(row):
        year = row["Date received"].year
        return (row["Product"], year, row["Company"]), 1
    
    
    def aggr_row_data(x):
        product=x[0][0].upper()
        year =x[0][1]
        total_complaints = sum(x[1])
        total_companies = len(x[1])
        highest_percent = round(100*max(x[1])/total_complaints)
        return (product, year, total_complaints, total_companies, highest_percent)

    df = sqlContext.read.csv(sys.argv[1] if len(sys.argv)>1 else 'complaints_small.csv', multiLine=True, \
        header=True, escape="\"", inferSchema=True)\
            .select(F.col("Date received"), F.col("Product"), F.col("Company")).cache()

    # df.show()
    # for x in df.rdd.take(20):
    #     print x

    rdd_complaints = df.rdd.map(map_row)

    rdd_aggr = rdd_complaints.reduceByKey(lambda x,y: x+y)\
        .map(lambda x: ((x[0][0], x[0][1]), x[1]))\
        .groupByKey().map(aggr_row_data)\
        .sortBy(lambda x: (x[0], x[1]))\
        .map(lambda x: ",".join(map(str,x)))
    
    rdd_result_heading = sc.parallelize(["product,year,total_complaints,total_companies,highest_percent"])
    
    rdd_result = rdd_result_heading.union(rdd_aggr).cache()
    # for x in rdd_result.take(20):
    #     print x
    rdd_result.saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'report')

if __name__ == '__main__':
    if sys.version_info[0] == 3: # if python3
        py3_solution()
    else: # if python2
        py2_solution()
