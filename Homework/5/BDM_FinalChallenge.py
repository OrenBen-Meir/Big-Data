from pyspark import SparkContext, SQLContext, RDD
from pyspark.sql import functions as F, Row, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, ArrayType
import sys
import os

def csv_df(sqlContext, filepath):
    return sqlContext.read.csv(filepath, multiLine=True, header=True, escape="\"", inferSchema=True)

if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    
    @F.udf(returnType=IntegerType())
    def date_to_year(x):
        from datetime import datetime
        return int(x.split(",")[0].split("/")[2])

    @F.udf(returnType=IntegerType())
    def boro_to_borocode(x):
        return {"NY": 1, "MN": 1, "BX": 2, "BRONX": 2, \
            "BK": 3, "K": 33, "KINGS": 3, "KING": 3, "BKLYN": 4, \
            "Q": 4, "QUEEN": 4, "QN": 4, "QNS": 4, "QU": 4, \
            "ST": 5, "SI": 5}.get(x, None)

    @F.udf(returnType=StringType())
    def trim_street(x):
        return ' '.join(x.upper().split())

    count_schema = StructType([StructField('PHYSICALID', IntegerType(), True),\
        StructField('year', IntegerType(), True),\
        StructField('count', IntegerType(), True)])


    df_violations = csv_df(sqlContext, os.path.join(sys.argv[1] if len(sys.argv) > 1 else "nyc_parking_violation", "*.csv"))\
        .select("Issue Date", "Street Name", "House Number", "Violation County")\
        .filter("`Issue Date` is not null and `Street Name` is not null and \
            `House Number` is not null and `Violation County` is not null ")\
        .withColumn("year", date_to_year(F.col("Issue Date")))\
        .filter("2015 <= year and year <= 2019")\
        .withColumn("BOROCODE", boro_to_borocode(F.col("Violation County")))\
        .filter("BOROCODE is not null")\
        .groupBy(trim_street(F.col("Street Name")).alias("Street_Name"), "BOROCODE")\
        .agg(F.collect_list(F.struct("year", "House Number")).alias("violations"))
    # df_violations.show()

    df_nyc_cscl_rows = csv_df(sqlContext, sys.argv[2] if len(sys.argv) > 2 else "nyc_cscl.csv")\
        .select("PHYSICALID", "FULL_STREE", "ST_LABEL", "BOROCODE", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN")\
        .filter("PHYSICALID is not null")
    # df_nyc_cscl_rows.show()

    df_nyc_cscl = df_nyc_cscl_rows.filter("FULL_STREE is not null").withColumn("Street_Name", trim_street(F.col("FULL_STREE")))\
        .union(df_nyc_cscl_rows.filter("ST_LABEL is not null").withColumn("Street_Name", trim_street(F.col("ST_LABEL"))))\
        .drop("FULL_STREE", "ST_LABEL")\
        .groupBy("Street_Name", "BOROCODE")\
        .agg(F.collect_list(F.struct("PHYSICALID", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN")).alias("csclS"))
    # df_nyc_cscl.show()

    df_join: DataFrame = df_nyc_cscl.join(df_violations, on=["Street_Name", "BOROCODE"], how="left_outer")

    def flatmap_to_id_years(row):
        def house_num_lst(x): # convert housenumber which is '-' seperated into a list of numbers for comparison
            return [int(n) for n in x.split("-") if n != ""]
        def house_limit_lst(x, is_high): # same as house_num_lst but for houselimits
            if x == '-' or x == None: 
                # if house limit is not around, if it is the lower bound, use -infinity, otherwise use infinity
                return [float('inf') if is_high else float('-inf')]
            return house_num_lst(x)
        counts = {}
        for cscl in row["csclS"]:
            for y in range(2015,2020):
                counts[(cscl["PHYSICALID"], y)] = 0
        if row["violations"] != None:
            violations_set = set(row["violations"])
            used_violations = set()
            for cscl in row["csclS"]:
                for y in range(2015,2020):
                    counts[(cscl["PHYSICALID"], y)] = 0
                for violation in violations_set:
                    try:
                        house_number = house_num_lst(violation["House Number"])
                    except (ValueError, TypeError) as e:
                        used_violations.add(violation)
                    try:
                        is_odd = house_number[len(house_number)-1]%2
                        if len(house_number) > 0:
                            if ((is_odd == 1 and house_limit_lst(cscl["L_LOW_HN"], False) <= house_number and \
                                    house_number <= house_limit_lst(cscl["L_HIGH_HN"], True)) or \
                                (is_odd == 0 and house_limit_lst(cscl["R_LOW_HN"], False) <= house_number and \
                                    house_number <= house_limit_lst(cscl["R_HIGH_HN"], True))):

                                counts[(cscl["PHYSICALID"], violation["year"])] += 1
                                used_violations.add(violation)
                    except:
                        continue
            violations_set.difference_update(used_violations)
        for x in counts.items():
            yield (x[0][0], x[0][1], x[1])

    # df_join.show(n=100)

    id_year_schema = StructType([StructField('PHYSICALID', IntegerType(), True),\
        StructField('year', IntegerType(), True)])

    def map_to_output_row(record):
        import numpy as np

        def calc_ols_coeff(pair_lst):
            if len(pair_lst) < 2:
                return "N/A"
            arr_pair_list = np.array(pair_lst, dtype=np.int64)
            arr_x = arr_pair_list[:,0]
            arr_y = arr_pair_list[:,1]
            n = len(arr_pair_list)
            
            bottom = n*np.sum(arr_x*arr_x) - np.sum(arr_x)**2
            if bottom == 0:
                return "N/A"
            if all(map(lambda x: x == 0, arr_y)):
                return 0
            top = n*np.sum(arr_x*arr_y) - np.sum(arr_x)*np.sum(arr_y)
            return str(round(top/bottom, 2))
        year_counts_tuples = [(x["year"], x["count"]) for x in record["yearcounts"]]
        year_counts_dict = dict(year_counts_tuples)
        L = [record[0], year_counts_dict[2015], year_counts_dict[2016], year_counts_dict[2017], \
            year_counts_dict[2018], year_counts_dict[2019], calc_ols_coeff(list(year_counts_tuples))]
        return ",".join(map(str,L))

    df_counts = sqlContext.createDataFrame(df_join.rdd.flatMap(flatmap_to_id_years), schema=count_schema)\
        .groupBy("PHYSICALID").agg(F.collect_list(F.struct("year", "count")).alias("yearcounts")).sort("PHYSICALID")\
        

    # df_counts.show(n=1000)
    rdd_counts: RDD = df_counts.rdd.map(map_to_output_row)
    # for c in rdd_counts.take(1000):
    #     print(c)
    # print(str(rdd_counts.count()) + " rows")

    rdd_counts.saveAsTextFile(sys.argv[3] if len(sys.argv) > 3 else 'final_output')
