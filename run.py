import time
import pyspark
from pyspark import SparkContext, SparkConf
import logging
import json

def square(num):
    return num * num

if __name__ == "__main__":
    print(pyspark.__version__)
    sc = SparkContext()
    destination = '/user/edill/test.parquet'

    # Create a dataframe
    rdd = sc.parallelize([_ for _ in range(1000)]).map(square)
    spark.createDataFrame(rdd).write.parquet(destination)

    # Write the dataframe
    sdf.write.parquet(destination)

    # Read it back in and show the contents
    sdf = sc.read.parquet(destination)
    sdf.limit(5).show()

