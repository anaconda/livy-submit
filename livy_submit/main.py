import time
import getpass
import os
import sys
import requests
import uuid
import requests_kerberos
import json
from argparse import ArgumentParser
from hdfs.ext.kerberos import KerberosClient
import logging


logger = logging.getLogger("livy-submit")
stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setLevel(logging.WARN)
logger.addHandler(stream_handler)
logger.setLevel(logging.WARN)


DEFAULT_NN_PORT = "50070"
DEFAULT_LIVY_PORT = "8998"


def _make_arg_parser():
    ap = ArgumentParser(description="CLI for submitting batch jobs to Livy")
    ap.add_argument(
        "-f", "--file", action="store", help=("The local target file to run with Spark")
    )
    ap.add_argument(
        "--config",
        action="append",
        help=("Spark configuration parameters. Formatted as key=value"),
        default=[],
    )
    ap.add_argument(
        "--hdfs-dir",
        action="store",
        help=(
            "Directory on hdfs to store input files for spark job. Defaults to "
            "/tmp/livy-submit/<uuid>"
        ),
        default=f"/tmp/{uuid.uuid4()}",
    )
    ap.add_argument(
        "-v",
        help="Increase verbosity: -v for info, -vv for debug",
        action="count",
        default=0,
    )
    ap.add_argument(
        "--namenode",
        required=True,
        action="store",
        help=(
            "The FQDN of the namenode where webHDFS is running. Optionally include the port "
            "after a colon, if different from 50070. e.g., nn1.dev.anaconda.com:50071"
        ),
    )

    ap.add_argument(
        "--livy-server",
        required=True,
        action="store",
        help=(
            "The livy hostname and port. e.g. --livy-server "
            "http://cdh-edge1.dev.anaconda.com:8998. Will assume default port of 8998 if none is "
            "provided"
        ),
    )

    ap.add_argument(
        "--job-name",
        action="store",
        help="The name of this spark job",
        default=f"Livy-submit job for {getpass.getuser()}",
    )
    return ap


def _parse_config(config: list) -> dict:
    cfg = {}
    for kv in config:
        try:
            k, v = kv.split("=")
        except ValueError:
            logging.error(
                f"Config values must be in the form key=value. You passed in {kv}"
            )
            raise
        cfg[k] = v
    return cfg


def cli():
    # get the argument parser and parse the args
    ap = _make_arg_parser()
    args = ap.parse_args()
    verbosity = args.v
    loglevel = 0
    if verbosity == 0:
        loglevel = logging.WARN
    elif verbosity == 1:
        loglevel = logging.INFO
    else:
        loglevel = logging.INFO
        if loglevel > 2:
            logger.warn("Passing in more than two '-v' will show no additional output")
    logger.setLevel(loglevel)
    stream_handler.setLevel(loglevel)
    logger.debug("Log level set to {loglevel}")

    # Show the debug level output while we're initializing the logger
    logger.debug("args: %s", args)

    spark_config = _parse_config(args.config)
    logger.debug("Spark config: %s", spark_config)

    hdfs_dir = args.hdfs_dir
    logger.info(f"Storing input files in {hdfs_dir}")

    run_file = os.path.abspath(args.file)
    logger.debug(f"Using {run_file} as the target for this Spark job")

    namenode_host = args.namenode
    try:
        namenode_host, namenode_port = namenode_host.split(":")
    except ValueError:
        logger.debug(f"Using default port {DEFAULT_NN_PORT} for hdfs")
        namenode_port = DEFAULT_NN_PORT
    namenode_connection_url = f"http://{namenode_host}:{namenode_port}"
    logger.debug(f"HDFS will use {namenode_connection_url}")

    livy_host = args.livy_server
    try:
        livy_host, livy_port = livy_host.split(":")
    except ValueError:
        logger.debug(f"Using default port {DEFAULT_LIVY_PORT} for livy")
        livy_port = DEFAULT_LIVY_PORT
    livy_connection_url = f"http://{livy_host}:{livy_port}"
    logger.debug(f"Livy will use {livy_connection_url}")

    job_name = args.job_name + " for: " + run_file
    logger.debug(f"Spark job name is {job_name}")
    run(
        hdfs_dir,
        spark_config,
        run_file,
        namenode_connection_url,
        livy_connection_url,
        job_name,
    )


def run(
    hdfs_dir,
    spark_config,
    run_file,
    namenode_connection_url,
    livy_connection_url,
    job_name,
):
    # 1. upload run_file to hdfs_dir
    client = KerberosClient(f"{namenode_connection_url}")
    client.upload(hdfs_path=hdfs_dir, local_path=run_file, overwrite=True)

    # 2. Format kerberos auth
    auth = requests_kerberos.HTTPKerberosAuth(
        mutual_authentication=requests_kerberos.REQUIRED, force_preemptive=True
    )

    headers = {"Content-Type": "application/json"}
    file_on_hdfs = f"hdfs://{hdfs_dir}/{os.path.basename(run_file)}"
    logger.info(f"Looking for {file_on_hdfs} for runfile")
    data = {"file": file_on_hdfs, "name": job_name, "conf": spark_config}
    json_data = json.dumps(data)
    # 3. Submit job to livy /batches endpoint
    connection_url = livy_connection_url + "/batches"
    r = requests.post(connection_url, headers=headers, auth=auth, data=json_data)
    print(r.text)

    def get_id(response_json):
        return response_json["id"]

    r.raise_for_status()
    response_json = r.json()
    submission_id = get_id(r.json())

    # 4. Monitor /batches job until done
    last_stop = 0
    for i in range(100):
        r = requests.get(
            connection_url + f"/{submission_id}/log", headers=headers, auth=auth
        )
        r.raise_for_status()
        logs_json = r.json()
        start = logs_json["from"]
        stop = logs_json["total"]
        logs = logs_json["log"]
        # print('logs array has %s elements' % len(logs))
        for line in logs[(1 + last_stop) :]:
            if line == "\nstderr: ":
                break
            print(line)
        last_stop = stop
        time.sleep(1)
