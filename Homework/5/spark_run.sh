export PYTHON_VERSION=3
spark-submit \
    --num-executors 6 \
    --executor-cores 5 \
    ---executor-memory 10G \
    BDM_FinalChallenge.py /data/share/bdm/nyc_parking_violation /data/share/bdm/nyc_cscl.csv oren_final_output

hdfs dfs -getmerge oren_final_output oren_final_output.csv
hdfs dfs -put oren_final_output.csv oren_final_output.csv

hdfs dfs -rmr oren_final_output
rm final_output.csv

