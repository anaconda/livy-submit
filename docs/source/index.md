# Welcome to livy-submit's documentation!

Our intent with livy-submit is that it behaves similarly to spark-submit in "cluster" deploy mode. 
To enable this functionality, we require WebHDFS or HttpFS to be (1) enabled on the Hadoop cluster and (2) visible from wherever you are running livy-submit. 
A description of webhdfs and httpfs can be found [here from Cloudera](https://community.cloudera.com/t5/Community-Articles/Comparison-of-HttpFs-and-WebHDFS/ta-p/245562).


## Brief intro by usage demonstration

livy-submit is a command line analog of spark-submit, focused on the Python / R use cases.
It requires an active Livy server to be functional.
Let's compare the command line invocation of spark-submit and livy-submit for the case where we want to run a Python script with the PySpark runtime being pulled from HDFS.
Using spark-submit, the CLI looks like this:

```bash
spark-submit --archives hdfs:///user/edill/example.tar.gz \
    --conf spark.pyspark.python=./example.tar.gz/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./example.tar.gz/bin/python \
    --deploy-mode cluster \
    test.py
```

Using livy-submit, the CLI looks like this:
```
livy-submit --archives hdfs:///user/edill/example.tar.gz \
    --conf spark.pyspark.python=./example.tar.gz/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./example.tar.gz/bin/python \
    --name livy submit test \
    --file test.py
```

Note the only difference being the explicit `--file` parameter that livy-submit takes.
These CLIs are otherwise identical.
Now, since we are aiming livy-submit at the Python / R audience, it would make sense to simplify their use case when using conda environments.
We've done that with livy-submit.
We automatically set the spark.pyspark.python and the spark.yarn.appMasterEnv.PYSPARK_PYTHON env vars for you, if you use the `--conda-env` flag. 
This allows the user to have a much simpler invocation:

```
livy-submit --conda-env hdfs:///user/edill/example.tar.gz \
    --name livy submit test \
    --file test.py
```

## Creating the conda environment

## Using with Anaconda Enterprise 5

1. Add the livy-submit package to your anaconda-project.yml
2. Add livy-submit.json to the platform
    a. Have an admin add it to all containers
    b. Add it to your individual project
3. Invoke as CLI or use as library

## livy-submit usage patterns

* [Using livy-submit from the command line](examples/as_cli)
* [livy-submit CLI docs](examples/cli_docs)
* [Using livy-submit as a library](examples/as_library)
* [Using livy-submit inside a REST API]()

## Indices and tables
* [index](genindex)
* [module index](modindex)
* [search](search)
* [API docs](livy_submit)
* [module list](modules)
