from pyspark.sql import SparkSession
import os
from transformations import clean_taxi_data, add_drive_duration, add_time_bins ,_schema
databricks_flag =False

def get_spark():
    global databricks_flag
    #for databricks
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        databricks_flag = True
        return SparkSession.builder.getOrCreate()
    return SparkSession.builder.master('local[*]').appName('NYC_TAXI_Pipline').getOrCreate()

def pipeline(in_path,out_path):
    spark=get_spark()

    print('loading data')
    df = spark.read.format('csv').option('header',True).schema(_schema).load(in_path)

    print('modyifing')
    df_modified = clean_taxi_data(df)
    df_modified = add_drive_duration(df_modified)
    df_final = add_time_bins(df_modified)

    print(f'saving to {out_path}')
    if databricks_flag:
        df_final.write.format('delta').option('header',True).mode('overwrite').save(out_path)
    else:
        df_final.write.format('parquet').mode('overwrite').save(out_path)

if __name__ == "__main__":
    #change if necessary
    IN = 'data/sampled_data.csv'
    OUT = 'data/output_data.parquet'

    pipeline(IN,OUT)

