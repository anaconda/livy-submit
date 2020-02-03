import requests
import json
import requests_kerberos
from typing import List, Tuple


class Batch:
    def __init__(self, id: str, name: str, appId: str, appInfo: dict, log: List, state: str):
        self.id = id
        self.appId = appId
        self.appInfo = appInfo
        self.log = log
        self.state = state
        self.name = name

    def __eq__(self, other):
        """Make an equality comparison ignoring the logs"""
        return (
            self.id == other.id
            and self.appId == other.appId
            and self.appInfo == other.appInfo
            and self.state == other.state
            and self.name == other.name
        )

    def __repr__(self):
        def _as_none(value):
            if value is None:
                return value
            else:
                # return a fully quoted string in the repr
                return f"'{value}'"

        return f"Batch(id={self.id}, name={_as_none(self.name)}, appId={_as_none(self.appId)}, appInfo={self.appInfo}, log='', state='{self.state}')"


class LivyAPI:
    def __init__(
        self,
        server_url: str,
        use_tls: bool = False,
        headers: dict = None,
        auth=None,
        verify: bool = True,
    ):
        """
        Parameters
        ----------
        server_url: The URL of the livy server. Should include protocol (http/https) and port
        headers, optional: A dictionary for the API request to Livy.
        auth, optional: An object for `requests` to use in its `auth` keyword.
        verify, optional: (True) Use TLS / SSL. (False) Ignore TLS / SSL errors
        """
        self._verify = verify

        if auth is None:
            auth = requests_kerberos.HTTPKerberosAuth(
                mutual_authentication=requests_kerberos.REQUIRED, force_preemptive=True
            )

        self._auth = auth

        self._base_url = "%s/batches" % server_url

        if headers is None:
            headers = {"Content-Type": "application/json"}

        self._headers = headers

    def all_info(
        self, from_index: int = None, size: int = None
    ) -> Tuple[int, int, List[Batch]]:
        """Returns all the active batch sessions.

        Handles: GET /batches

        TODO: consider adding a "logs" parameter that does not include
        the logs in the json output

        Parameters
        ----------
        from, optional: The start index to fetch batches
        size, optional: The number of batches to return

        Returns
        -------
        int: The start index of the fetched sessions
        int: The number of fetched sessions
        dict: List of Batch objects, indexed by their id
        """
        data = {}
        if from_index:
            data["from"] = from_index
        if size:
            data["size"] = size

        response = self._request("get", self._base_url, data=data)
        info = {batch["id"]: Batch(**batch) for batch in response["sessions"]}
        return response["from"], response["total"], info

    def info(self, batch_id: int) -> Batch:
        """Returns the batch session information.

        Handles: GET /batches/{batchId}
        """
        url = "%s/%s" % (self._base_url, batch_id)
        response = self._request("get", url)
        return Batch(**response)

    def state(self, batch_id: int) -> Tuple[int, str]:
        """Returns the state of batch session.

        Handles: GET /batches/{batchId}/state

        Parameters
        ----------
        batch_id: The ID of the livy /batches job.

        Returns
        -------
        int: Batch session id
        str: The current state of batch session
        """
        url = "%s/%s/state" % (self._base_url, batch_id)
        response = self._request("get", url)
        return response["id"], response["state"]

    def submit(
        self,
        file: str,
        name: str = None,
        driverMemory: str = None,
        driverCores: int = None,
        executorMemory: str = None,
        executorCores: int = None,
        numExecutors: int = None,
        archives: List[str] = None,
        queue: str = None,
        conf: dict = None,
        args: List[str] = None,
        pyFiles: List[str] = None,
    ) -> Batch:
        """
        Submit a batch job to the Livy server

        Parameters
        ----------
        file : str
            The file that should be executed during your Spark job. This file
            path must be accessible from the nodes that run your Spark driver/executors.
            It is likely that this means that you will need to have pre-uploaded your file
            to hdfs and then use the hdfs path for this `file` variable.
            e.g.: file='hdfs://user/testuser/pi.py'
        name : str, optional
            The name that your Spark job should have on the Yarn RM. The supplied
            name must be unique for all known Livy jobs (starting, running, or completed) or
            a ValueError is thrown. If the name is not provided the job name will
            be listed as None in Livy and as the filename in the Yarn RM.
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

        Returns
        -------
        Batch
            The information that is immediately available on submission to Livy. e.g.:
            {'id': 20,
             'state': 'starting',
             'appId': None,
             'appInfo': {'driverLogUrl': None, 'sparkUiUrl': None},
             'log': ['stdout: ', '\nstderr: ', '\nYARN Diagnostics: ']}

        """
        # Get a dictionary of all of the non-None values that are passed in
        local_items = locals()
        data = {}
        for var, val in local_items.items():
            if val is None or var == "self":
                continue
            data[var] = val
        #         print(data)

        # check for collision with existing batch name
        if name is not None:
            _, _, batches = self.all_info()
            for num,batch in batches.items():
                if (batch.name is not None) and (batch.name == name):
                    msg = f'''The batch name '{name}' is already in use by Livy Batch Job {batch.id}.
You can either
  * change the name of this batch job,
  * kill Batch Job {batch.id} with .kill({batch.id}),
  * or wait for {batch.id} to finish and be removed from the output of .all_info()'''
                    raise ValueError(msg)

        # Submit the data dict to the Livy Batches API to create a batch job
        response = self._request("post", self._base_url, data=data)
        return Batch(**response)

    def log(
        self, batch_id: int, starting_line: int = None, num_lines: int = None
    ) -> Tuple[int, int, int, List[str]]:
        """
        Get the log lines from the batch job represented by `session_id`

        Will return up to the last 100 lines of the log. For more log lines
        consider calling this function multiple times and caching the lines locally
        or consider using the Spark History Server for more.

        TODO: Submit bug report to Livy since both `starting_line` and `num_lines` appear
        to be ignored in the Livy REST API

        Parameters
        ----------
        batch_id: The job to get info for
        starting_line: (optional) The offset from zero for the log lines
        num_lines: (optional) Max number of log lines to return

        Returns
        -------
        int: The batch id
        int: Offset from start of log
        int: Number of log lines
        list of strings: The log lines
        """
        data = {}
        if starting_line is not None:
            data["from"] = starting_line
        if num_lines is not None:
            data["size"] = num_lines
        url = "%s/%s/log" % (self._base_url, batch_id)
        response = self._request("get", url, data=data)

        # Split the logs into stdout/stderr
        logs = response["log"]
        stderr_idx = logs.index("\nstderr: ")
        stdout_logs = logs[:stderr_idx]
        stderr_logs = logs[stderr_idx:]

        return (
            response["id"],
            response["from"],
            response["total"],
            stdout_logs,
            stderr_logs,
        )

    def kill(self, batchId: int):
        """
        Terminate Livy job and delete its state from the Livy server

        Parameters
        ----------
        batchId: The batch id that you want to kill
        """
        url = "%s/%s" % (self._base_url, batchId)
        response = self._request("delete", url)
        return response

    def _request(self, rest_action: str, url: str, data: dict = None) -> dict:
        """
        Helper function to handle some boilerplate get requests to the Livy server

        Parameters
        ----------
        rest_action : {'get', 'post', 'delete'}
            The REST API action to perform. Must be one of 'get', 'post', or 'delete'
        url : str
            The url to hit
        data : dict
            The parameters that should be sent to the Livy REST endpoint


        Returns
        -------
        dict
            The raw response from the Livy API as a Python dict
        """
        if data is None:
            data = {}
        # Convert the REST payload into JSON.
        json_data = json.dumps(data)
        # Find the right function to call, 'get', 'post', or 'delete'
        func = getattr(requests, rest_action.lower())
        # interact with Livy
        resp = func(
            url,
            auth=self._auth,
            data=json_data,
            headers=self._headers,
            verify=self._verify,
        )
        # Make sure that our request was successful
        resp.raise_for_status()
        return resp.json()
