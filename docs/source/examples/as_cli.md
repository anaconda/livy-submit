# Use livy-submit as a cli

Let's say you have an existing PySpark script that does some data reduction you need as part of a data pipeline.
Maybe you've ssh'd in to one of your Hadoop edge nodes and have set this script up via crontab. 
This is a pretty common pattern that we've seen and isn't necessarily bad, but it definitely does not scale well.
There are a few alternatives that you have to using cron.
1. Maybe you're interested in exploring airflow.
This is a reasonable choice and gives you quite a bit more flexibility in how you manage your data engineering and data science pipelines.
2. Maybe you've got an existing enterprise scheduler (like Tivoli) that you want to make use of for this PySpark job.
3. Or maybe you're one of our existing Anaconda Enterprise 5 (AE5) customers, then you can make use of the built-in [job-scheduling feature of AE5](https://enterprise-docs.anaconda.com/en/latest/data-science-workflows/deployments/schedule-deploy.html) to handle cron-like scheduling on the AE5 platform.

To move this from a cron job using spark-submit to somewhere else using livy-submit you need a few things in place:
1. hostname & port of Livy server
2. hostname & port of Hadoop namenode (for webhdfs)
3. Conda environment that encapsulates all dependencies for the PySpark script

#1 and #2 are pretty straightforward. These can be placed in the ~/.livy-submit/config.json file, for example:
```
{
    "namenode_url": "http://namenode.edill.anaconda.com:50070",
    "livy_url": "http://livy.edill.anaconda.com:8998",
    "driver_memory": "4g",
    "driver_cores": 2,
    "executor_memory": "2g",
    "executor_cores": 1
}
```

These parameters can also be provided on the command line:
```
livy --namenode-url http://namenode.test.anaconda.com:50070 \
    --livy-url http://livy.test.anaconda.com:50070
    ...
```

For #3, the recommended path forward is to create a conda environment that contains all of the required dependencies for your PySpark script. 
If you don't need anything above and beyond what PySpark includes, then you don't need to provide a conda environment at all.
If you do have additional dependencies, then your best bet is going to be to create the conda environment and then pack it using [conda-pack](https://conda.github.io/conda-pack/#commandline-usage).
For example, let's say you need pandas and scikit-learn for your PySpark job.
You'd create an environment like so:
```
conda create -n my_env scikit-learn pandas conda-pack -c conda-forge
conda activate my_env
conda pack -n my_env -o my_env.zip
```

Then you'd provide this environment to `livy-submit` via the `--conda-env` flag. 
`livy-submit` will attempt to upload this archive to HDFS for you if you provide it with a local path.
It is generally much better to upload `my_env.zip` to HDFS yourself and then provide the HDFS path to `--conda-env`.
That way, you dont need to wait for the conda-env to upload to HDFS every time.

Once your environment has been made, you're ready to submit your example script to Livy.
For this example, let's modify an example from [this blog post](https://quasiben.github.io/blog/2016/4/15/conda-spark/):

```python
# test_spark.py
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName("get-hosts")

sc = SparkContext(conf=conf)

def noop(x):
    import socket
    import pandas
    import sklearn
    info = [
        socket.gethostname(),
        sys.executable,
        pandas.__file__,
        sklearn.__file__
    ]
    return ','.join(info)

rdd = sc.parallelize(range(1000), 100)
hosts = rdd.map(noop).distinct().collect()
print(hosts)
```

The above script, invoked with spark-submit:
```
spark-submit \
    --archives hdfs:///user/edill/testenv.zip \
    --conf spark.pyspark.python=./testenv.zip/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./testenv.zip/bin/python \
    --deploy-mode cluster \
    --master yarn test_spark.py
```

from an edge node produces the following (slightly formatted for comprehension):
```
['ip-10-0-2-111,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000002/testenv.zip/bin/python,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000002/testenv.zip/lib/python3.7/site-packages/pandas/__init__.py,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000002/testenv.zip/lib/python3.7/site-packages/sklearn/__init__.py', 
    'ip-10-0-2-111,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000003/testenv.zip/bin/python,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000003/testenv.zip/lib/python3.7/site-packages/pandas/__init__.py,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000003/testenv.zip/lib/python3.7/site-packages/sklearn/__init__.py', 
    'ip-10-0-2-89,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000004/testenv.zip/bin/python,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000004/testenv.zip/lib/python3.7/site-packages/pandas/__init__.py,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0230/container_1561571912988_0230_01_000004/testenv.zip/lib/python3.7/site-packages/sklearn/__init__.py'
    ]
```

**You can similarly do this with livy-submit:**
```
livy submit \
    --conda-env hdfs:///user/edill/testenv.zip \
    --name spark_env_test
    --file test_spark.py
```

Which produces the following output (lightly formatted):
```
['ip-10-0-2-89,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000002/testenv.zip/bin/python,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000002/testenv.zip/lib/python3.7/site-packages/pandas/__init__.py,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000002/testenv.zip/lib/python3.7/site-packages/sklearn/__init__.py', 
    'ip-10-0-2-89,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000003/testenv.zip/bin/python,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000003/testenv.zip/lib/python3.7/site-packages/pandas/__init__.py,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000003/testenv.zip/lib/python3.7/site-packages/sklearn/__init__.py',
     'ip-10-0-2-111,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000004/testenv.zip/bin/python,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000004/testenv.zip/lib/python3.7/site-packages/pandas/__init__.py,
    /mnt/yarn/usercache/edill/appcache/application_1561571912988_0231/container_1561571912988_0231_01_000004/testenv.zip/lib/python3.7/site-packages/sklearn/__init__.py']
```