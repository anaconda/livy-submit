# livy-submit
CLI for submitting batch spark jobs to livy. Think spark-submit

## Dev
To use, pip install in editable mode `pip install -e .`

Then you can use the `livy-submit` CLI

(Possibly incomplete) Requirements are:
* python-hdfs
* requests-kerberos
* reuests

## Example
Run this code snippet to submit a spark job to the livy endpoint in the SA Lab cluster in the Anaconda datacenter

```
export HDFS_USER=<your kerberos hdfs user>
livy-submit \
    --config spark.pyspark.python=/opt/envs_dir/anaconda3/bin/python\
    --config spark.driver.memory=4g\
    --hdfs-dir=/user/${HDFS_USER}/livy-submit\
    -vv \
    --f spark.driver.memory=4g\
    --hdfs-dir=/user/${HDFS_USER}/livy-submit\
    --file pi.py\
    --namenode cdh-nn1.dev.anaconda.com\
    --livy-server=cdh-edge1.dev.anaconda.com
```

Once you run this code, look for the tracking url for oyur job. This is the place where you can
find the spark logs. For example,
```
         tracking URL: http://cdh-nn1.dev.anaconda.com:8088/proxy/application_1538148161343_0037/
```

You'll also likely need to kill this execution with ctrl+c because it just loops for 100 seconds.

## Things to add

* Ability to upload custom conda environment to HDFS and pull that environment down to the Spark
  Driver and executors during their execution. This let's the user control their own conda
  environment for execution and therefore does not rely on their Hadoop administrator to deploy
  their desired environment.
* Testing, obviously :D
* Add sub-arg-parse pieces so that you could do something like `livy-submit monitor <job_id>`. This
  would then tail the logs that are available at the `/batches/<job id>/log` endpoint
* Add sub-argparse piece so that you can kill a submitted job `livy-submit kill <job_id>`. This
  would then DELETE on the `/batches/<job id>` endpoint
* Fully expose all parameters on the POST to /batches endpoint to the command line submit piece.
  There are a lot...
* Write code to look for and load a configuration file that lets the user specify things like the
  namenode url and port, the livy url and port, default spark parameters, etc.
