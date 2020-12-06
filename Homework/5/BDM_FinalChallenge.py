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
    
    def map_row_add_year(x): # add year from issued date in violations data
        from datetime import datetime
        from pyspark.sql import Row
        row_dict = x.asDict()
        if x["Issue Date"] is not None:
            row_dict["year"] = datetime.strptime(row_dict["Issue Date"].split(",")[0], "%m/%d/%Y").year
        return Row(**row_dict)

    # read every csv file in a chosen directory (marked by *.csv) to a dataframe
    # select chosen fields, convert to rdd
    # add year to row, then filter year tp be from 2015 to 2019
    # map to the form (whitespace_trimmed_streetname, boro_number), row
    # group by key and map into the form (whitespace_trimmed_streetname, boro_number), (mode_number, violation row collection)
    # mode_number is 1 to indicate violations when joining
    rdd_violations = csv_df(sqlContext, os.path.join(sys.argv[1] if len(sys.argv) > 1 else "nyc_parking_violation", "*.csv"))\
        .select("Issue Date", "Street Name", "House Number", "Violation County").rdd\
        .filter(lambda x: None not in [x["Issue Date"], x["Street Name"], x["House Number"]])\
        .map(map_row_add_year)\
        .filter(lambda x: 2015 <= x["year"] and x["year"] <= 2019)\
        .map(lambda x: ((' '.join(x["Street Name"].upper().split()),\
            {"NY": 1, "MN": 1, \
                "BX": 2, "BRONX": 2, \
                "BK": 3, "K": 33, "KINGS": 3, "KING": 3, "BKLYN": 4, \
                "Q": 4, "QUEEN": 4, "QN": 4, "QNS": 4, "QU": 4, \
                "ST": 5, "SI": 5}.get(x["Violation County"], None)\
            ), x))\
        .filter(lambda x: x[0][1] != None)\
        .groupByKey().map(lambda x: (x[0], (1, x[1])))

    # read chosen csv into a dataframe
    # select required fields and convert to rdd
    # filter where PHYSICALID and borocode is not empty
    # flatmap into a list of tuples of the form ((whitespace_trimmed_streetname, boro_number), cscl row)
    # group by key and map into the form (whitespace_trimmed_streetname, boro_number), (mode_number, collection of cscl rows)
    # mode_number is 0 to indicate cscl when joining
    rdd_nyc_cscl_rows = csv_df(sqlContext, sys.argv[2] if len(sys.argv) > 2 else "nyc_cscl.csv")\
        .select("PHYSICALID", "FULL_STREE", "ST_LABEL", "BOROCODE", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN")\
        .rdd

    rdd_nyc_cscl = rdd_nyc_cscl_rows.filter(lambda x: None != x["PHYSICALID"])\
        .flatMap(lambda x: [\
            ((' '.join(street.split()), x["BOROCODE"]), x) \
            for street in [x["FULL_STREE"], x["ST_LABEL"]] \
            if street != None\
        ])\
        .groupByKey().map(lambda x: (x[0], (0, x[1])))

    # default (physical id, year), counts where counts is 0, to be later added to a union of cscl and violation data to add
    # locations with no violation
    rdd_nyc_cscl_base_zero = rdd_nyc_cscl_rows\
        .flatMap(lambda x: [((x["PHYSICALID"], y), 0) for y in range(2015, 2020)])
    
    # join sorted union of cscl info and violations
    def map_partitions_join_cscl_violations(records):
        def house_num_lst(x): # convert housenumber which is '-' seperated into a list of numbers for comparison
            return [int(n) for n in x.split("-") if n != ""]
        def house_limit_lst(x, is_low): # same as house_num_lst but for houselimits
            if x == '-' or x == None: 
                # if house limit is not around, if it is the lower bound, use -infinity, otherwise use infinity
                return [float('-inf') if is_low else float('inf')]
            return house_num_lst(x)
        last_cscls = None
        for r in records:
            mode = r[1][0]
            if mode == 1: # if r is from violations 
                if last_cscls != None and r[0] == last_cscls[0]: # keys must match
                    for violation_row in r[1][1]:
                        # house_number is a number list from "House Number" from violations. 
                        # Skips if it turns "House Number" can't be converted to a number list or is empty 
                        try:
                            house_number = house_num_lst(violation_row["House Number"])
                            if len(house_number) > 0:
                                for cscl_row in last_cscls[1]: # search street centerline data such that house number
                                    if ((house_number[len(house_number)-1]%2 == 1 and \
                                            house_limit_lst(cscl_row["L_LOW_HN"], True) <= house_number and \
                                            house_number <= house_limit_lst(cscl_row["L_HIGH_HN"], False)) or \
                                        (house_number[len(house_number)-1]%2 == 0 and \
                                            house_limit_lst(cscl_row["R_LOW_HN"], True) <= house_number and \
                                            house_number <= house_limit_lst(cscl_row["R_HIGH_HN"], False))):
                                        yield (cscl_row["PHYSICALID"], violation_row["year"]), 1
                                        break
                        except:
                            continue
            else:
                last_cscls = (r[0], r[1][1])
    
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
        L = [entry[0], year_counts[2015], year_counts[2016], year_counts[2017], \
            year_counts[2018], year_counts[2019], calc_ols_coeff(list(year_counts.items()))]
        return ",".join(map(str,L))
    
    #efficiently join by unioning rdd violations, sort by key, then mapping partitions to emit values based on join condition.
    # the emmitted valies are of the form (physical id, year), 1
    # union with rdd_nyc_cscl_base_zero rdd so that physical ids without violations are counted
    # reduce by key so we get year counts for each year of a street segment
    # map into the form (physical id, (year, counts))
    # then we group by key to collect all of the year counts for physical id
    # map the phys id grouping into a comma seperated list string which is a csv line
    rdd_location_year_counts: RDD = (rdd_nyc_cscl + rdd_violations).sortByKey()\
        .mapPartitions(map_partitions_join_cscl_violations).union(rdd_nyc_cscl_base_zero)\
        .reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .groupByKey().sortByKey().map(map_to_output_row)
    
    # for line in rdd_location_year_counts.take(1000):
    #     print(line)
    # print("rows count:", rdd_location_year_counts.count())

    rdd_location_year_counts.saveAsTextFile(sys.argv[3] if len(sys.argv) > 3 else 'final_output')
