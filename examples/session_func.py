from livy_submit import LivySession, pyspark, krb
import numpy as np

krb.kinit_username('instructor', 'anaconda')
session = LivySession('http://livy.training.anaconda.com:8998', name='Test2')

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

@pyspark(session)
def func(a, n):
    ## This step is not strictly necessary as
    ## the decorator ensures that spark is loaded,
    ## but is considered best practice.
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    x = [{'a':'1', 'b':2}, {'c':pi(), 'd':pi_np(n)}]
    arr = np.array([1, 2, a])

    table = spark.table('autompg')
    df = table.groupby('origin').mean('mpg').toPandas()
    return x, arr, df

result = func(4, n=int(1e4))
print(result)


print(session.delete())
