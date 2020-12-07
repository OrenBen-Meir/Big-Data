# This is a convenience script to autorun a spark job and generate the output csv file

# spark submit
export PYTHON_VERSION=3
spark-submit \
    --num-executors 6 \
    --executor-cores 5 \
    --executor-memory 10G \
    BDM_FinalChallenge_BenMeir.py /data/share/bdm/nyc_parking_violation /data/share/bdm/nyc_cscl.csv oren_final_output

# getmerge output folder and save to hdfs
hdfs dfs -getmerge oren_final_output oren_final_output.csv
hdfs dfs -put oren_final_output.csv oren_final_output.csv

# clean local and hdfs files
hdfs dfs -rm -r oren_final_output
rm oren_final_output.csv

