# CSC445 Big Data - Huy Vo
# Name: Oren Ben-Meir
# EMPLID: 14144874
# Final Project

# To run, please type the following
'''
export PYTHON_VERSION=3
spark-submit \
    --num-executors 6 \
    --executor-cores 5 \
    --executor-memory 10G \
    BDM_FinalChallenge_BenMeir.py <parking violation folder path> <Street Centerline CSCL folder path> <output folder path to getmerge>

In this case:

Parking violation folder = '/data/share/bdm/nyc_parking_violation'
CSCL File = '/data/share/bdm/nyc_cscl.csv'
Output Folder = 'oren_final_output'

To copy/paste: 

export PYTHON_VERSION=3
spark-submit \
    --num-executors 6 \
    --executor-cores 5 \
    --executor-memory 10G \
    BDM_FinalChallenge_BenMeir.py /data/share/bdm/nyc_parking_violation /data/share/bdm/nyc_cscl.csv oren_final_output
'''

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
        '''Parse issued date field into year'''
        from datetime import datetime
        return int(x.split(",")[0].split("/")[2])

    @F.udf(returnType=IntegerType())
    def boro_to_borocode(x):
        '''Convert boro string to borocode'''
        return {"NY": 1, "MN": 1, "BX": 2, "BRONX": 2, \
            "BK": 3, "K": 33, "KINGS": 3, "KING": 3, "BKLYN": 4, \
            "Q": 4, "QUEEN": 4, "QN": 4, "QNS": 4, "QU": 4, \
            "ST": 5, "SI": 5, "R": 5, "RICHMOND": 5}.get(x, None)

    @F.udf(returnType=StringType())
    def trim_street(x):
        '''Remove repeating whitespaces'''
        return ' '.join(x.upper().split())

    count_schema = StructType([StructField('PHYSICALID', IntegerType(), True),\
        StructField('year', IntegerType(), True),\
        StructField('count', IntegerType(), True)])

    # get the violation data into the dataframe
    # filter for null values  and make sure home address has the right format
    # extract year from issued date and make sure the year is in the 2015 to 2019 range
    # convert boro to borocode and only make sure nyc boroughs go through by using the null filter
    # trim streetname of any extra white spaces and collect violations based on a common streetname and borough.
    # The violations collection row is called `violations`
    df_violations = csv_df(sqlContext, os.path.join(sys.argv[1] if len(sys.argv) > 1 else "nyc_parking_violation", "*.csv"))\
        .select("Issue Date", "Street Name", "House Number", "Violation County").dropna()\
        .filter(F.col("House Number").rlike("^(\d+)((-(\d+))*)$"))\
        .withColumn("year", date_to_year(F.col("Issue Date")))\
        .filter("2015 <= year and year <= 2019")\
        .withColumn("BOROCODE", boro_to_borocode(F.col("Violation County")))\
        .dropna(subset=["BOROCODE"])\
        .groupBy(trim_street(F.col("Street Name")).alias("Street_Name"), "BOROCODE")\
        .agg(F.collect_list(F.struct("year", "House Number")).alias("violations"))
    # df_violations.show()


    #Read street centerline data
    #Select needed fields and make sure physical id is not null
    df_nyc_cscl_rows = csv_df(sqlContext, sys.argv[2] if len(sys.argv) > 2 else "nyc_cscl.csv")\
        .select("PHYSICALID", "FULL_STREE", "ST_LABEL", "BOROCODE", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN")\
        .dropna(subset=["PHYSICALID"])
    # df_nyc_cscl_rows.show()

    # Seperate duplicate the cscl fields based on transorming FULL_STREE or ST_LABEL into street name and then union them.
    # The street names are trimmed and FULL_STREE and ST_LABEL get dropped.
    # trim streetname of any extra white spaces and collect cscl data based on a common streetname and borough.
    # The violations collection row is called `cscls`
    df_nyc_cscl = \
        df_nyc_cscl_rows.dropna(subset=["FULL_STREE"]).withColumn("Street_Name", trim_street(F.col("FULL_STREE"))).drop("FULL_STREE")\
        .union(df_nyc_cscl_rows.dropna(subset=["ST_LABEL"]).withColumn("Street_Name", trim_street(F.col("ST_LABEL"))).drop("ST_LABEL"))\
        .groupBy("Street_Name", "BOROCODE")\
        .agg(F.collect_list(F.struct("PHYSICALID", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN")).alias("csclS"))
    # df_nyc_cscl.show()

    # join violations and cscl based on street and boro, left outer so streets without violations are included
    df_join: DataFrame = df_nyc_cscl.join(df_violations, on=["Street_Name", "BOROCODE"], how="left_outer")

    def flatmap_to_id_years_counts(row):
        def house_num_lst(x): # convert housenumber which is a hyphen seperated mumber into a list of numbers for comparison
            return [int(n) for n in x.split("-") if n != ""]
        def house_limit_lst(x, is_high): # same as house_num_lst but for houselimits
            if x == '-' or x == None: 
                # if house limit is not around, if it is the lower bound, use -infinity, otherwise use infinity
                return [float('inf') if is_high else float('-inf')]
            return house_num_lst(x)
        import re
        house_num_pattern = re.compile("^(\d+)((-(\d+))*)$")# house number must be numbers seperated by hyphens
        counts = {} # dictionary with key being phycal id and year and value is number of violations
        for cscl in row["csclS"]:
            for y in range(2015,2020): # Violation count by default is 0
                counts[(cscl["PHYSICALID"], y)] = 0
        if row["violations"] != None: # if violations arent empty
            violations_set = set(row["violations"]) # create a set for faster means of removing data
            used_violations = set() # violations no longer needed
            for cscl in row["csclS"]:
                if any(map(lambda c: cscl[c] == None or not house_num_pattern.match(cscl[c]), \
                    ["L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN"])):
                    continue # skip cscls with unusable house number limits
                for violation in violations_set: # for every available violations
                    # housenumber is a list of numbers that were dash seperated (if no dash a singular list)
                    house_number = house_num_lst(violation["House Number"]) 
                    if len(house_number) > 0: # make sure list is not empty
                        is_odd = house_number[len(house_number)-1]%2
                        if ((is_odd == 1 and house_num_lst(cscl["L_LOW_HN"]) <= house_number and \
                                house_number <= house_num_lst(cscl["L_HIGH_HN"])) or \
                            (is_odd == 0 and house_num_lst(cscl["R_LOW_HN"]) <= house_number and \
                                house_number <= house_num_lst(cscl["R_HIGH_HN"]))):

                            counts[(cscl["PHYSICALID"], violation["year"])] += 1 # increment counts
                            used_violations.add(violation) # violation already matched with location so no longer needed
            violations_set.difference_update(used_violations) # clean violations_set of used violations
        for x in counts.items(): # generate output of the form (phys id, year, counts)
            yield (x[0][0], x[0][1], x[1])

    # df_join.show(n=100)

    def map_to_output_row(record): # creates a csv row in the form 'physical id, 2015 counts, ...., 2019 counts, ols coefficient
        import numpy as np

        def calc_ols_coeff(pair_lst): # calculate ols coefficient, there are floating point error bugs
            if len(pair_lst) < 2:
                return "N/A"
            arr_pair_list = np.array(pair_lst, dtype=np.int64)
            arr_x = arr_pair_list[:,0]
            arr_y = arr_pair_list[:,1]
            n = len(arr_pair_list)
            
            bottom = n*np.sum(arr_x*arr_x) - np.sum(arr_x)**2
            if bottom == 0:
                return "N/A"
            top = n*np.sum(arr_x*arr_y) - np.sum(arr_x)*np.sum(arr_y)
            return str(round(top/bottom, 2))
        year_counts_tuples = [(int(x["year"]), int(x["count"])) for x in record["yearcounts"]]
        year_counts_dict = dict(year_counts_tuples)
        L = [record[0], year_counts_dict[2015], year_counts_dict[2016], year_counts_dict[2017], \
            year_counts_dict[2018], year_counts_dict[2019], calc_ols_coeff(year_counts_tuples)]
        return ",".join(map(str,L))

    # take the joined dataframe and flatmap it such that it returns a generator of the counts of [physical id,year] pair.
    # Then group by physical id and year to sum all counts of such since physical ids and years may repeat
    # group by phycial id where the counts for each year are aggregated into the yearcounts row
    # sort by physical id
    df_counts = sqlContext.createDataFrame(df_join.rdd.flatMap(flatmap_to_id_years_counts), schema=count_schema)\
        .groupBy("PHYSICALID", "year").agg(F.sum("count").alias("count"))\
        .groupBy("PHYSICALID").agg(F.collect_list(F.struct("year", "count")).alias("yearcounts")).sort("PHYSICALID")\
    
    # for x in df_counts.rdd.take(1000):
    #     print("\ncounts: ", x)
        
    # df_counts.show(n=1000)

    # take this phys id aggregation and convert to rdd so it will be used to create a csv string.
    # It will have a row for physical id, rows for each year counts and calculate the ols coefficient for the year counts
    rdd_counts: RDD = df_counts.rdd.map(map_to_output_row)

    # for c in rdd_counts.take(1000):
    #     print(c)
    # print(str(rdd_counts.count()) + " rows")

    # save to csv
    rdd_counts.saveAsTextFile(sys.argv[3] if len(sys.argv) > 3 else 'final_output')
