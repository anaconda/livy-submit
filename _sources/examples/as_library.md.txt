# Use livy-submit as a Python Library

livy-submit is both a CLI tool and a Python library. 
The CLI piece makes use of the Python API so all functionality that is exposed by the CLI is also available inside of a Jupyter notebook or a Python file (or another Python library if you wish to build on top of livy submit). 
There are three modules inside of livy submit that you'll want to be aware of if you're going to be importing livy submit into your Jupyter notebook or Python file:

1. `livy_submit/livy_api.py`: This is a Python API around all of the Livy REST API /batches endpoints. 
There are a number of endpoints on Livy to manage batch jobs and all are made available via the livy_api.py module inside of livy_submit.
There is a LivyAPI class inside of this module. 
Use this class for full access to the /batches operations on the Livy REST API. 
See below for more detail.

2. `livy_submit/hdfs_api.py`: This is a wrapper around another Python HDFS library that exposes some useful functionality:
    a. An upload function that can upload a local file to an hdfs directory
    b. A delete function that can be used to remove an hdfs directory
    c. A get_client function that can be used to get the underlying Python HDFS client for full HDFS file system access

3. `livy_submit/krb.py`: This is where you'd go for two kinit functions.
    a. `kinit_keytab`: Use a keytab to create a Kerberos TGT for your session
    b. `kinit_username`: Use a username and password to create a Kerberos TGT for your session

## LivyAPI
To use the LivyAPI directly, you'll need to know the url+port for your Livy server.

```python
from livy_submit import LivyAPI
server_url = 'http://ip-172-31-20-241.ec2.internal:8998'
 
livy_api = LivyAPI(server_url=server_url)
```
Now that we have the livy_api, we can interact with it. 
With the following six functions you can get quite sophisticated in terms of what you can do with submitting spark job, monitoring their progress and checking their final state. 
I suspect it would not be too difficult to build your own basic pipeline based on this API and batch job submission.

1. I can get the info of all available sessions via the `livy_api.all_info()` function. 
This will return a list of Batch namedtuples that have the following fields (in order):
    a. `id`: The Livy batch ID
    b. `appId`: The Yarn application ID, if it has been set yet
    c. `appInfo`: Dictionary that contains the driver log URL and the spark UI URL
    d. `log`: Up to the last 100 log lines from the Yarn application
    e. `state`: The current state of the Batch job. Should be one of "starting", "running, "finished" or "error" (though I may be wrong on exactly what these options are. They're not documented by Livy anywhere).
2. I can get the info of a specific session via the `livy_api.info(batch_id)` function. This returns one Batch object or raises a 403 HTTP error
3. I can get the state of a specific session via the `livy_api.state(batch_id)` function. This returns a tuple of (batch_id, state)
4. I can get the most recent log lines via the `livy_api.log(batch_id)` function. 
5. I can kill the job via the `livy_api.kill(batch_id)` function.
6. I can submit a new job to be executed via the `livy_api.submit()` function. This submit function takes a lot of parameters:

```
Parameters
----------
name : str
    The name that your Spark job should have on the Yarn RM
file : str
    The file that should be executed during your Spark job. This file
    path must be accessible from the nodes that run your Spark driver/executors.
    It is likely that this means that you will need to have pre-uploaded your file
    to hdfs and then use the hdfs path for this `file` variable.
    e.g.: file='hdfs://user/testuser/pi.py'
driverMemory : str, optional
    e.g. 512m, 2g
    Amount of memory to use for the driver process, i.e. where
    SparkContext is initialized, in the same format as JVM memory
    strings with a size unit suffix ("k", "m", "g" or "t")
driverCores : int, optional
        Number of cores to use for the driver process, only in cluster mode.
executorMemory : str, optional
    e.g. 512m, 2g
    Amount of memory to use per executor process, in the same format as
    JVM memory strings with a size unit suffix ("k", "m", "g" or "t")
executorCores : int, optional
    The number of cores to use on each executor
archives : List of strings
    Archives to be used in this session. Same deal as the `file` parameter. These
    archives likely need to already be uploaded to HDFS unless the files already
    exist on your Yarn nodes or if you have something like NFS available on all
    of the Yarn nodes.
queue : str
    The YARN queue that your job should run in
conf : dict
    Additonal spark configuration properties. Any valid variable listed in the
    spark configuration for your version of spark. See all here
    https://spark.apache.org/docs/latest/configuration.html
    e.g. {'spark.pyspark.python': '/opt/anaconda3/bin/python'}
args : list of strings
    Extra command line args for the application. If your python main is expecting
    command line args, use this variable to pass them in.
pyFiles: list of strings
    Python files to be used in this session. Same deal as the `file` parameter.
    These archives need to be already uploaded to HDFS.
```
