from pyspark import SparkContext, SQLContext, RDD
from pyspark.sql import functions as F, Row, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
import sys
import os

def csv_df(sqlContext, filepath):
    return sqlContext.read.csv(filepath, multiLine=True, header=True, escape="\"", inferSchema=True)

def violation_data_df(sparkcontext, sqlContext, *filenames):
    filenames = ["2015.csv", "2016.csv", "2017.csv", "2018.csv", "2019.csv"]
    violation_rdds = [
        csv_df(sqlContext, os.path.join(sys.argv[1] if len(sys.argv) > 1 else "nyc_parking_violation", fname))\
            .select(F.col("House Number"),F.col("Street Name"), F.col("Violation County"),\
                F.year(F.to_date(F.split(F.col("Issue Date"), ",")[0], "MM/dd/yyyy")).alias("year"))\
            .rdd
        for fname in filenames]

    schema = StructType([StructField('House Number', StringType(), True),\
        StructField('Street Name', StringType(), True),\
        StructField('Violation County', StringType(), True),\
        StructField('year', IntegerType(), True)])
    rdd_violations = sparkcontext.union(violation_rdds)
    return sqlContext.createDataFrame(rdd_violations, schema)

if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    @F.udf("string")
    def sql_to_upper(x):
        if x == None:
            return None
        return x.upper()
    sqlContext.registerFunction("to_upper", sql_to_upper)

    filenames = ["2015.csv", "2016.csv", "2017.csv", "2018.csv", "2019.csv"]
    violation_rdds = [
        csv_df(sqlContext, os.path.join(sys.argv[1] if len(sys.argv) > 1 else "nyc_parking_violation", fname)).rdd
        for fname in filenames
    ]
    
    def map_row_add_year(x):
        from datetime import datetime
        from pyspark.sql import Row
        row_builder = x.asDict()
        if x["Issue Date"] is not None:
            row_builder["year"] = datetime.strptime(row_builder["Issue Date"].split(",")[0], "%m/%d/%Y").year
        if x["Street Name"] is not None:
            row_builder["Street Name"] = row_builder["Street Name"].upper()
        return Row(**row_builder)
    rdd_violations = sc.union(violation_rdds)\
        .filter(lambda x: None not in [x["Issue Date"], x["Street Name"], x["House Number"]])\
        .map(map_row_add_year)\
        .filter(lambda x: 2015 <= x["year"] and x["year"] <= 2019)

    df_nyc_cscl = csv_df(sqlContext, sys.argv[2] if len(sys.argv) > 2 else "nyc_cscl.csv")\
        .select("PHYSICALID", "ST_LABEL", "BOROCODE", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN")

    def map_partitions_to_rdd_violations(rows):
        county_to_boro_codes = {"NY": 1, "BX": 2, "BK": 3, "Q": 4, "ST": 5}
        for row in rows:
            boro = county_to_boro_codes.get(row["Violation County"], None)
            if boro == None:
                continue
            yield (row["Street Name"], boro), row
        
    rdd_violations = rdd_violations.mapPartitions(map_partitions_to_rdd_violations)
    rdd_nyc_cscl = df_nyc_cscl.rdd.map(lambda x: ((x["ST_LABEL"], x["BOROCODE"]), x))

    rdd_cscl_violations = rdd_nyc_cscl.join(rdd_violations)

    def map_partitions_to_phys_id_and_year(items):                
        for x in items:
            v = x[1]
            cscl_row = v[0]
            violation_row = v[1]
            try:
                house_number = int(violation_row["House Number"])
                if ((house_number%2 == 1 and int(cscl_row["L_LOW_HN"]) <= house_number and house_number <= int(cscl_row["L_HIGH_HN"])) or \
                    (house_number%2 == 0 and int(cscl_row["R_LOW_HN"]) <= house_number and house_number <= int(cscl_row["R_HIGH_HN"]))):
                    yield (cscl_row["PHYSICALID"], violation_row["year"]), 1
            except (ValueError, TypeError) as e:
                continue
    
    def map_to_output_row(entry):
        ols_coeff = "NaN"
        year_counts = dict(entry[1])
        return [entry[0], year_counts.get(2015, "NaN"), year_counts.get(2016, "NaN"), year_counts.get(2017, "NaN"), \
            year_counts.get(2018, "NaN"), year_counts.get(2019, "NaN"), ols_coeff]
                
    rdd_location_year_counts: RDD = rdd_cscl_violations.mapPartitions(map_partitions_to_phys_id_and_year)\
        .reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .groupByKey().sortByKey().map(map_to_output_row)
    
    output_schema = StructType([StructField('PHYSICALID', LongType(), True),\
        StructField('COUNT_2015', StringType(), True),\
        StructField('COUNT_2016', StringType(), True),\
        StructField('COUNT_2017', StringType(), True),\
        StructField('COUNT_2018', StringType(), True),\
        StructField('COUNT_2019', StringType(), True),\
        StructField('OLS_COEF', StringType(), True)])
    
    df_output = sqlContext.createDataFrame(rdd_location_year_counts, schema=output_schema)
    
    df_output.show()
    df_output.write.csv(sys.argv[3] if len(sys.argv) > 3 else 'final_output', header=False)