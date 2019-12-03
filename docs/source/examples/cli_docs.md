# Use livy-submit as a CLI tool

If you have placed a livy submit configuration file in the expected location `~/.livy-submit.json` then you will be able to execute livy from the command line after you have installed it into your AE project. 
`livy` has four subcommands: `info`, `submit`, `kill` and `log`.

`livy info`: Livy info is used to get information on live sessions and recently completed sessions. 
It can be used in one of three ways:

`livy info`: Show me all available information
`livy info --state`: Show me all available job IDs with their corresponding job state. The job state will generally be "starting, success or errored"
`livy info jobID`: Show me all info for the one job I specify with jobID
`livy info jobID --state`: Show me the state for the one job I specify with jobID

Full CLI help output:
```
(anaconda50_hadoop) livy info -h
usage: livy-submit info [-h] [--state] [batchId]

positional arguments:
  batchId     The Livy batch ID that you want information for

optional arguments:
  -h, --help  show this help message and exit
  --state     Only show the current status of the job
```

## `livy submit`

livy submit has a lot of CLI options. Let's look at the help to see just what it looks like:
```
(anaconda50_hadoop) livy submit -h
usage: livy-submit submit [-h] --name NAME --file FILE
                          [--archives ARCHIVES]
                          [--driver-memory DRIVERMEMORY]
                          [--driver-cores DRIVERCORES]
                          [--executor-memory EXECUTORMEMORY]
                          [--executor-cores EXECUTORCORES]
                          [--num-executors NUMEXECUTORS] [--queue QUEUE]
                          [--args ARGS] [--py-files PYFILES]
                          [--conf CONF]

optional arguments:
  -h, --help            show this help message and exit
  --name NAME           The name that your Spark job should have on the
                        Yarn RM
  --file FILE           The file that should be executed during your
                        Spark job. If this file is local it will be
                        uploaded to a temporary path on HDFS so it will
                        be accessible from your Spark driver and
                        executors
  --archives ARCHIVES   An archive to be used in this session. Parameter
                        can be used multiple times to provide multiple
                        archives. Same deal as the `file` parameter.
                        These archives will be uploaded to HDFS.
  --driver-memory DRIVERMEMORY
                        e.g. 512m, 2g. Amount of memory to use for the
                        driver process, i.e. where SparkContext is
                        initialized, in the same format as JVM memory
                        strings with a size unit suffix ("k", "m", "g" or
                        "t"). Overrides settings contained in config
                        files.
  --driver-cores DRIVERCORES
                        Number of cores to use for the driver process,
                        only in cluster mode. Overrides settings
                        contained in config files.
  --executor-memory EXECUTORMEMORY
                        e.g. 512m, 2g. Amount of memory to use per
                        executor process, in the same format as JVM
                        memory strings with a size unit suffix ("k", "m",
                        "g" or "t"). Overrides settings contained in
                        config files.
  --executor-cores EXECUTORCORES
                        The number of cores for each executor. Overrides
                        settings contained in config files.
  --num-executors NUMEXECUTORS
                        Number of executors to launch for this session
  --queue QUEUE         The YARN queue that your job should run in
  --args ARGS           Extra command line args for the application. If
                        your python main is expecting command line args,
                        use this variable to pass them in as space
                        delimited. Will use shlex to split args
  --py-files PYFILES    Python files to be used in this session.
                        Parameter can be used multiple times to provide
                        multiple archives. Same deal as the `file`
                        parameter. These archives will be uploaded to
                        HDFS.
  --conf CONF           Additional Spark/Yarn/PySpark configuration
                        properties. Parameter can be used multiple times
                        to provide multiple parameters. Values should
                        take the form of --conf spark.property=value
```

## `livy log`
Use the log subcommand to do one of two things:

`livy log LIVY_JOB_ID`: Get the last 100 lines of the Spark log
`livy log -f LIVY_JOB_ID`: Tail the log and get a continuous update from the Spark log.
These are pretty self-explanatory. Try them out and see how they work. Full livy log help text:

 



## `livy kill`

This is equivalent to using the yarn CLI to kill an application by its applicationID: `yarn application -kill applicationID`. 
To use this, you'll need the livy job ID and then you just kill it: `livy kill LIVY_JOB_ID`. 
Note that once you kill the livy app, it removes the logs from the Livy server.
```
(anaconda50_hadoop) livy info 39
Batch(id=39, appId='application_1544723249474_0277', appInfo={'driverLogUrl': None, 'sparkUiUrl': 'http://ip-172-31-20-241.ec2.internal:20888/proxy/application_1544723249474_0277/'}, log='', state='success')

(anaconda50_hadoop) livy kill 39
Job killed:
{'msg': 'deleted'}

(anaconda50_hadoop) livy info 39
Traceback (most recent call last):
  File "/opt/continuum/anaconda/envs/anaconda50_hadoop/bin/livy", line 11, in <module>
    sys.exit(cli())
  File "/opt/continuum/anaconda/envs/anaconda50_hadoop/lib/python3.6/site-packages/livy_submit/cli.py", line 612, in cli
    cfg["func"](**cfg)
  File "/opt/continuum/anaconda/envs/anaconda50_hadoop/lib/python3.6/site-packages/livy_submit/cli.py", line 210, in _livy_info_func
    resp = api_instance.info(batchId)
  File "/opt/continuum/anaconda/envs/anaconda50_hadoop/lib/python3.6/site-packages/livy_submit/livy_api.py", line 90, ininfo
    response = self._request("get", url)
  File "/opt/continuum/anaconda/envs/anaconda50_hadoop/lib/python3.6/site-packages/livy_submit/livy_api.py", line 281, in _request
    resp.raise_for_status()
  File "/opt/continuum/anaconda/envs/anaconda50_hadoop/lib/python3.6/site-packages/requests/models.py", line 935, in raise_for_status
    raise HTTPError(http_error_msg, response=self)
requests.exceptions.HTTPError: 404 Client Error: Not Found for url: http://ip-172-31-20-241.ec2.internal:8998/batches/39
```


Full livy kill CLI help output:
```
(anaconda50_hadoop) livy kill -h
usage: livy-submit kill [-h] batchId

positional arguments:
  batchId     The Livy batch ID that you want to terminate

optional arguments:
  -h, --help  show this help message and exit
```

