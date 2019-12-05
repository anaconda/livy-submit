# livy-submit
[![Build Status](https://travis-ci.com/Anaconda-Platform/livy-submit.svg?branch=master)](https://travis-ci.com/Anaconda-Platform/livy-submit)

General documentation found [here](https://anaconda-platform.github.io/livy-submit/). Example usage patterns can be found [here](https://anaconda-platform.github.io/livy-submit/#livy-submit-usage-patterns)


Our intent with livy-submit is that it behaves similarly to spark-submit in "cluster" deploy mode. 
To enable this functionality, we require WebHDFS or HttpFS to be (1) enabled on the Hadoop cluster and (2) visible from wherever you are running livy-submit. 
A description of webhdfs and httpfs can be found [here from Cloudera](https://community.cloudera.com/t5/Community-Articles/Comparison-of-HttpFs-and-WebHDFS/ta-p/245562).

## Network requirements

1. Your client must be able to see the livy server
2. Your client must be able to see the Namenode so you can upload your python files to HDFS so that
   Spark can pull them down at runtime.
3. You must have webHDFS enabled. (or HttpFS, though that has not been tested yet)


## Build docs

1. Create conda environment:

```
conda create -n livy-submit-dev --file requrements-dev.txt --file requirements.txt
```

2. Build apidoc files (`conda activate livy-submit-dev`, first)

```
sphinx-apidoc -f -o docs/source livy_submit
```

3. Build docs

```
cd docs
make html
```

4. View docs by opening docs/build/index.html in a web browser