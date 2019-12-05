# livy-submit
CLI for submitting batch spark jobs to livy. Think spark-submit

Docs on internal Anaconda Confluence

1. General documentation found [here](https://anaconda.atlassian.net/wiki/spaces/ProductMarketing/pages/132612097/Livy+submit+documentation)
2. Example of wrapping a REST API around livy-submit and using it on AE5 found [here](https://anaconda.atlassian.net/wiki/spaces/ProductMarketing/pages/124059651/AE5+REST+API+deployment+using+livy-submit+for+batch+job+submission)

# Old docs that are basically fully folded in to the Confluence docs:

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

## Network requirements

1. Your client must be able to see the livy server
2. Your client must be able to see the Namenode so you can upload your python files to HDFS so that
   Spark can pull them down at runtime.
3. You must have webHDFS enabled. (or HttpFS, though that has not been tested yet)

## Usage

### Configuration
livy-submit is designed to pick up some global configuration from config files. This is so the users of livy-submit do not need to know all of the intimate details of where different services are running, or to let a platform administrator configure some of the Spark settings once for all of the platform users to use. There is nothing that is set in any of these config files that cannot be overridden on the command line by the user at runtime. The config from the sparkmagic config file (default: `~/.sparkmagic/config.json`) is the lowest priority, followed by the config from the livy submit config file (default: `~/.livy-submit.json`), with the arguments passed in on the command line taking the highest priority.

#### configuration from the sparkmagic config json file
Since you may already have `sparkmagic` installed, livy-submit can read information from that config file. livy-submit will, by default, look for a file in the location: `~/.sparkmagic/config.json`. You can also control this behavior by passing the path to your config file in at the command line: `livy --sparkmagic-config /path/to/config.json`. If livy-submit finds a sparkmagic config file at the location specified by default or by the user then it will attempt to pull the following values out:

1. Livy server URL
2. Livy server Port
3. Default spark parameters

The Livy server URL and Port will be pulled from the key `kernel_python_credentials` inside of the sparkmagic config.json file and the Default spark parameters will be pulled from the key `session_configs`. Have a look at the `~/.sparkmagic/config.json` file that is likely inside of your AE5 editor session if you're using the Hadoop-Spark template. While your config will likely be a little different, you should find these two keys present there:

```
{
  "kernel_python_credentials" : {
    "url": "http://ip-172-31-20-241.ec2.internal:8998",
    "auth": "Kerberos"
  },

  ...,

  "session_configs": {
    "driverMemory": "1000M",
    "executorCores": 2
  },
```

#### configuration from the livy-submit config json file
Your platform admin may have configured a livy-submit config file for you to use. If the platform admin has configured that file to be mounted inside of your container at `~/.livy-submit.json` then livy-submit will automatically pick it up and load parameters out of it. If the platform admin has place the file somewhere else inside of your container, or if you have a config file that you'd like to use inside of your AE5 project, then you have a few options. You can set the (verbosely named) environmental variable, `LIVY_SUBMIT_CONFIG=/path/to/config.json`, or you can provide the information at the command line, `livy --livy-submit-config /path/to/config.json`. If you don't to manually set the environmental variable or add this flag at the command line, then you also have the option of hard-coding this in your anaconda-project.yml file:

```yaml
variables:
  LIVY_SUBMIT_CONFIG:
    description: Location of config file for livy submit
    default: /opt/continuum/project/livy-submit.json
```

Regarding which parameters can be set inside of the config file, any parameter for any of the execution functions hanging off of `livy` can be set. These execution functions are `livy info`, `livy submit`, `livy kill` and `livy log`. For `livy info` you can set

To explain this a little more, consider the following

### `livy submit`

The main entry point for this code is `livy submit` which lets you execute PySpark files in batch mode. The main flag that you will care about when executing PySpark code is `--file`, which should contain the main entry point into your PySpark job. You must also provide a name for your spark job via the `--name` flag. That looks like this:

```
livy submit --name my-spark-job --file pi.py
```

You can add supporting files to this with the `--archives` flag. `--archives` can be used multiple times to add multiple archives. These archives can probably be any format, but I'd stick with zip files. These zip files will be extracted into your YARN container working directory so you can reference them via local paths in your code.

```
livy submit --name my-spark-job --file pi.py --archives supporting-archive.zip --archives supporting-archive2.zip
```

You can also add a few special-cased configuration objects via the command line:

```
livy submit --file pi.py --driver-memory 4g --driver-cores 2 --executor-memory 2g --executor-cores 1 --num-executors 10 --queue prod
```

You can pass command line arguments to your execution file by using the `--args` flag. You should pass this a quoted string. If you were running your execution file locally like this:

```
pi.py 1 2 --var1 1 --var2 1,2
```

Then the corresponding `livy-submit` submission would look like this:

```
livy submit --name my-prog-with-args --file pi.py --args "1 2 --var1 1 --var2 1,2"
```

Finally, there is a configuration file that you can stuff all of these parameters (and a few more). `livy-submit` will look for a file located here by default: `~/.livy-submit.json`. You can configure this with the `--livy-submit-config` flag. `livy-submit` will also look for a sparkmagic config.json file since that may contain the URL/port for your Livy server. `livy-submit` looks by default in `~/.sparkmagic/config.json` but can be pointed else where with the `--sparkmagic-config` flag on the command line. These, taken together, look like this:

```
livy --sparkmagic-config ~/.config.json \
    --livy-submit-config ~/.config/livy-submit.json \
    submit \
    --name my-job-with-custom-config
    --file pi.py
```

In the above example, note specifically that the two config flags must come before the `submit` subcommand.

The CLI has a number of defaults built in. The livy submit config file can set any of these values. Additionally, values specified on the command line will take precedence over values set in the config file. For example, my livy-submit.json file looks like this:

```
{
    "namenode_url": "http://ip-172-31-20-241.ec2.internal:50070",
    "livy_url": "http://ip-172-31-20-241.ec2.internal:8998",
    "driver_memory": "4g",
    "driver_cores": 2,
    "executor_memory": "2g",
    "executor_cores": 1,
    "conf": {
        "spark.pyspark.python": "/mnt/anaconda/anaconda2/bin/python",
    }
}
```

This above file lets me simplify my command line invocation to:

```
livy submit --name livy-submit-test --file test/data/pi.py
```

Without this config file, it would look something like this:
```
livy --namenode-url http://ip-172-31-20-241.ec2.internal:50070 \
    --livy-url http://ip-172-31-20-241.ec2.internal:8998 \
    submit \
    --name livy-submit-test \
    --file test/data/pi.py \
    --driver-memory 4g \
    --driver-cores 2 \
    --executor-memory 2g \
    --executor-cores 1
```

NOTE: You can only set arbitrary spark configuration via the livy-submit.json file. Parsing that on the command line is sufficiently complex that we are not shipping that capability yet. If you need to be able to specify arbitrary spark parameters at the command line (e.g., `--conf spark.pyspark.python='/opt/anaconda/bin/python'`) then please reach out to your Customer Success Manager and we can discuss implementing this functionality for you.

### `livy log`

There are two ways to access logs for submitted batch jobs: `livy log <batchId>` and `livy log <batchId> -f`. The `-f` flag will "follow" the logs by querying the Livy API for its logs and then showing the new lines.

## Dev
To use, pip install in editable mode `pip install -e .`

Then you can use the `livy-submit` CLI

(Possibly incomplete) Requirements are:
* python-hdfs
* requests-kerberos
* requests

## Example
Run this code snippet to submit a spark job to the livy endpoint in the SA Lab cluster in the
Anaconda datacenter

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
