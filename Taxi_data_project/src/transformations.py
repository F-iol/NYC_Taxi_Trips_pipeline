from pyspark.sql.functions import col,unix_timestamp,expr,hour,dayofweek,when
import pyspark.sql.functions as F

_schema = 'VendorID int,tpep_pickup_datetime timestamp,tpep_dropoff_datetime timestamp,passenger_count int,trip_distance double,pickup_longitude double,pickup_latitude double,RateCodeID int,' \
'store_and_fwd_flag string,dropoff_longitude double,dropoff_latitude double,payment_type double,fare_amount double,extra double,mta_tax double,tip_amount double,tolls_amount double,improvement_surcharge double,total_amount double'
    

def clean_taxi_data(df):
    # cleaning negative/unreal values
    return df.filter(
        (col('total_amount')>0) &
        (col('passenger_count')>0) &
        (col('trip_distance')>0) &
        (col('fare_amount')>0) &
        (col('tpep_pickup_datetime') < col('tpep_dropoff_datetime'))
    )

#add drive length
def add_drive_duration(df):
    pickup = unix_timestamp(col('tpep_pickup_datetime'))
    dropof = unix_timestamp(col('tpep_dropoff_datetime'))
    return df.withColumn('duration_min',F.round((dropof-pickup)/60,2))

#extract detailed time info 
def add_time_bins(df):
    return (df.withColumn('hour',hour('tpep_pickup_datetime'))
            .withColumn('day_of_week',dayofweek('tpep_pickup_datetime'))
            .withColumn('is_weekend',when(col('day_of_week').isin(6,7),True).otherwise(False))
            )


    
