from pyspark import SparkContext, SQLContext, RDD
from pyspark.sql import functions as F, Row, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
import sys
import os

def csv_df(sqlContext, filepath):
    return sqlContext.read.csv(filepath, multiLine=True, header=True, escape="\"", inferSchema=True)

if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    
    def map_row_add_year(x):
        from datetime import datetime
        from pyspark.sql import Row
        row_dict = x.asDict()
        if x["Issue Date"] is not None:
            row_dict["year"] = datetime.strptime(row_dict["Issue Date"].split(",")[0], "%m/%d/%Y").year
        return Row(**row_dict)

    rdd_violations = csv_df(sqlContext, os.path.join(sys.argv[1] if len(sys.argv) > 1 else "nyc_parking_violation", "*.csv"))\
        .select("Issue Date", "Street Name", "House Number", "Violation County").rdd\
        .filter(lambda x: None not in [x["Issue Date"], x["Street Name"], x["House Number"]] and \
            x["Violation County"] in {"NY", "BX", "BK", "Q", "ST"})\
        .map(map_row_add_year)\
        .filter(lambda x: 2015 <= x["year"] and x["year"] <= 2019)\
        .map(lambda x: ((' '.join(x["Street Name"].upper().split()), ["NY", "BX", "BK", "Q", "ST"].index(x["Violation County"])+1), x))\
        .groupByKey().map(lambda x: (x[0], (1, x[1])))

    rdd_nyc_cscl = csv_df(sqlContext, sys.argv[2] if len(sys.argv) > 2 else "nyc_cscl.csv")\
        .select("PHYSICALID", "FULL_STREE", "ST_LABEL", "BOROCODE", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN")\
        .rdd\
        .filter(lambda x: \
            None not in [x["PHYSICALID"], x["FULL_STREE"], x["BOROCODE"], x["L_LOW_HN"], x["L_HIGH_HN"], x["R_LOW_HN"], x["R_HIGH_HN"]] \
            and any([x != None for x in [x["FULL_STREE"], x["ST_LABEL"]]]))\
        .flatMap(lambda x: [((' '.join(x["FULL_STREE"].split()), x["BOROCODE"]), x), ((' '.join(x["ST_LABEL"].split()), x["BOROCODE"]), x)])\
        .groupByKey().map(lambda x: (x[0], (0, x[1])))
    
    def map_partitions_join_cscl_violations(records):
        def house_num_lst(x):
            return [int(n) for n in x.split("-") if n != ""]
        def house_limit_lst(x, is_low):
            if x == '-':
                return [float('-inf') if is_low else float('inf')]
            return house_num_lst(x)
        last_cscls = None
        for r in records:
            mode = r[1][0]
            if mode == 1:
                if last_cscls != None and r[0] == last_cscls[0]:
                    for violation_row in r[1][1]:
                        try:
                            house_number = house_num_lst(violation_row["House Number"])
                            if len(house_number) == 0:
                                continue
                        except:
                            continue
                        for cscl_row in last_cscls[1]:
                            try:
                                if ((house_number[len(house_number)-1]%2 == 1 and \
                                        house_limit_lst(cscl_row["L_LOW_HN"], True) <= house_number and \
                                        house_number <= house_limit_lst(cscl_row["L_HIGH_HN"], False)) or \
                                    (house_number[len(house_number)-1]%2 == 0 and \
                                        house_limit_lst(cscl_row["R_LOW_HN"], True) <= house_number and \
                                        house_number <= house_limit_lst(cscl_row["R_HIGH_HN"], False))):
                                    yield (cscl_row["PHYSICALID"], violation_row["year"]), 1
                                    break
                            except (ValueError, TypeError, IndexError) as e:
                                continue
            else:
                last_cscls = (r[0], set(r[1][1]))
                for cscl_row in last_cscls[1]:
                    for y in range(2015, 2020):
                        yield (cscl_row["PHYSICALID"], y), 0
    
    def map_to_output_row(entry):
        import numpy as np
        def calc_ols_coeff(pair_lst):
            if len(pair_lst) < 2:
                return "N/A"
            arr_pair_list = np.array(pair_lst)
            arr_x = arr_pair_list[:,0]
            arr_y = arr_pair_list[:,1]
            n = len(arr_pair_list)
            
            bottom = n*np.sum(arr_x*arr_x) - np.sum(arr_x)**2
            if bottom == 0:
                return "N/A"
            top = n*np.sum(arr_x*arr_y) - np.sum(arr_x)*np.sum(arr_y)
            return str(round(top/bottom, 2))
        year_counts = dict(entry[1])
        for year in range(2015,2020):
            if year not in year_counts:
                year_counts[year] = 0
        return [entry[0], year_counts[2015], year_counts[2016], year_counts[2017], \
            year_counts[2018], year_counts[2019], calc_ols_coeff(list(year_counts.items()))]
    
    rdd_location_year_counts: RDD = rdd_nyc_cscl.union(rdd_violations).sortByKey()\
        .mapPartitions(map_partitions_join_cscl_violations)\
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
    
    # df_output.show(1000)
    # print(df_output.count())
    df_output.write.csv(sys.argv[3] if len(sys.argv) > 3 else 'final_output', header=False)