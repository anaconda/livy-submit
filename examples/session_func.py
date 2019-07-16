from livy_submit import LivySession, krb
import numpy as np
from pprint import pprint

krb.kinit_username('instructor', 'anaconda')

def pi(n=int(1e3)):
    prod = 1.0
    for i in range(1,n):
        term1 = 4*(i**2)
        prod = prod * term1/(term1-1)
    return 2*prod

def pi_np(n=int(1e4)):
    x = np.arange(1,n)
    t = 4*x*x
    y = t / (t - 1)
    z = 2.0 * y.prod()
    return z

with LivySession('http://livy.training.anaconda.com:8998') as pyspark:
    @pyspark
    def func(a, n):
        ## This step is not strictly necessary as
        ## the decorator ensures that spark is loaded,
        ## but is considered best practice.
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        # python on the spark master
        x = [{'a':'1', 'b':2}, {'c':pi(), 'd':pi_np(n)}]
        arr = np.array([1, 2, a])

        # hive tables
        table = spark.table('autompg')
        hdf = table.groupby('origin').mean('mpg').toPandas()

        return x, arr, hdf

    result = func(4, int(1e4))

pprint(result)
