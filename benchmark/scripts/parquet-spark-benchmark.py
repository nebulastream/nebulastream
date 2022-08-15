from pyspark.sql import SparkSession
import datetime


def f(x):
    # do nothing
    return


def main():
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "4g") \
        .appName('my-cool-app') \
        .getOrCreate()

    a = datetime.datetime.now()
    parq = spark.read.parquet('data_parquet_spark/area1.parquet')
    #parq = spark.read.parquet('data_parquet_spark/parquet-big.parquet')
    parq.foreach(f)
    b = datetime.datetime.now()
    delta = b - a
    print(delta.total_seconds(), 's')
    print(delta.total_seconds() * 1000, 'ms')


if __name__ == '__main__':
    main()
