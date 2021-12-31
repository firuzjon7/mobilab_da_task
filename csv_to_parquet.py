from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os


if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet", master="spark://172.19.0.2:7077")
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField('VendorID', IntegerType(), False),
        StructField('tpep_pickup_datetime', TimestampType(), False),
        StructField('tpep_dropoff_datetime', TimestampType(), False),
        StructField('Passenger_count', IntegerType(), False),
        StructField('Trip_distance', FloatType(), False),
        StructField('PULocationID', IntegerType(), False),
        StructField('DOLocationID', IntegerType(), False),
        StructField('RateCodeID', IntegerType(), False),
        StructField('Payment_type', IntegerType(), False),
        StructField('Fare_amount', FloatType(), False),
        StructField('Extra', FloatType(), False),
        StructField('MTA_tax', FloatType(), False),
        StructField('Improvement_surcharge', FloatType(), False),
        StructField('Tip_amount', FloatType(), False),
        StructField('Tolls_amount', FloatType(), False),
        StructField('Total_amount', FloatType(), False)])
    rdd = sc.textFile(os.path.join(
        '/output/Yellow-00000-of-00001.csv')).map(lambda line: line.split(","))
    DataFrame = sqlContext.createDataFrame(rdd, schema)
    DataFrame.write.parquet(os.path.join(
        '/output/Yellow-00000-of-00001-parquet'))
