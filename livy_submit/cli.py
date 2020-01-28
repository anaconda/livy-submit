from argparse import ArgumentParser
from urllib.parse import urlparse
from os.path import expanduser, dirname
from typing import Dict, List
from . import livy_api, hdfs_api
from pprint import pprint, pformat
import json
import os
import shlex
import sys
import pdb
import logging
import time


CONFIG_PATH_ENV_VAR = "LIVY_SUBMIT_CONFIG"
DEFAULT_SPARKMAGIC_CONFIG_PATH = "~/.sparkmagic/config.json"
DEFAULT_LIVY_SUBMIT_CONFIG_PATH = "~/.livy-submit.json"
logger = None


def _init_logger(loglevel):
    # set up the logger
    global logger
    logger = logging.getLogger("livy-submit")

    # clear all handlers
    for handler in logger.handlers:
        logger.removeHandler(handler)
    logger.setLevel(loglevel)
    format_string = ""
    #     format_string = "%(asctime)-15s %(levelname)s: %(message)s"
    formatter = logging.Formatter(fmt=format_string)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(loglevel)
    stream_handler.setFormatter(fmt=formatter)

    logger.addHandler(stream_handler)

    logger.debug("Log level set to %s" % logging.getLevelName(loglevel))


def _sparkmagic_config(config_path: str) -> Dict:
    """Read the sparkmagic configuration file for the
    Spark defaults and the Livy server url/port

    Returns
    -------
    dict: Keys are "spark_config", "livy_url" and "livy_port"
    """
    if not os.path.exists(config_path):
        logger.warn("%s not found. Cannot load sparkmagic defaults", config_path)
        return {}
    with open(config_path, "r") as f:
        cfg = json.loads(f.read())
    return_vals = {}
    return_vals = cfg.get("session_configs", {})
    livy_server = cfg.get("kernel_python_credentials", {}).get("url")
    if livy_server is not None:
        url, port = livy_server.rsplit(":", maxsplit=1)
    else:
        err_str = (
            "'kernel_python_credentials' not found in sparkmagic "
            "configuration (%s). Unable to automatically determine "
            "the location of your Livy server" % config_path
        )
        logger.error(err_str)
        url, port = None, None

    return_vals.update({"livy_url": url, "livy_port": port})

    return return_vals


def _livy_submit_config(config_path: str) -> Dict:
    """Read the config json for livy-submit

    Returns
    -------
    dict: Known keys are:
        namenode_url
        livy_url
        driverMemory
        driverCores
        executorMemory
        executorCores
        numExecutors
        archives
        queue
        conf
        args
    """
    options = [("cli", config_path)]
    env_path = os.environ.get(CONFIG_PATH_ENV_VAR)
    if env_path is not None:
        options.append(("env var", env_path))
    path_to_use = None
    for location, path in options:
        if os.path.exists(path):
            logger.debug("using config set by %s found at %s" % (location, path))
            path_to_use = path
            break
        else:
            logger.debug("config from %s not found at %s" % (location, path))

    if not path_to_use:
        logger.error(
            "No config file found at any specified paths. Cannot load "
            "defaults for livy submit. Run with -v for more config info"
        )
        return {}

    with open(path_to_use, "r") as f:
        return json.loads(f.read())


def _base_parser():
    """Configure the base parser that the other subcommands will inherit from

    Configs will be loaded in this order with items coming later overwriting
    items coming earlier if the same key is present in multiple locations:
    1. sparkmagic conf
    2. livy-submit conf
    3. command line args


    Returns
    -------
    ArgumentParser
    """
    ap = ArgumentParser(
        prog="livy-submit",
        description="CLI for interacting with the Livy REST API",
        add_help=False,
    )
    ap.add_argument(
        "--sparkmagic-config",
        action="store",
        default=expanduser(DEFAULT_SPARKMAGIC_CONFIG_PATH),
        help=(
            "The location of the sparkmagic configuration file. "
            "Will extract the Livy url/port and the spark defaults "
            "set in `session_configs`"
        ),
    )
    ap.add_argument(
        "--livy-submit-config",
        action="store",
        default=expanduser(DEFAULT_LIVY_SUBMIT_CONFIG_PATH),
        help="The location of the livy submit configuration file",
    )
    ap.add_argument(
        "--namenode-url",
        action="store",
        help=(
            "The url of the namenode. Should include protocol "
            "(http/https) and port (50070)"
        ),
    )
    ap.add_argument(
        "--livy-url",
        action="store",
        help=(
            "The url of the Livy server. Should include protocol "
            "(http/https) and port (8998)"
        ),
    )
    ap.add_argument(
        "--pdb",
        action="store_true",
        default=False,
        help="Drop into a debugger on client-side exception",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="logging defaults to info. Switch to debug with -v/--verbose",
    )
    ap.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="logging defaults to info. Switch to warn with -q/--quiet",
    )
    ap.add_argument(
        '-k',
        '--ignore-ssl-errors',
        action='store_false',
        default=True,
        dest='verify',
        help="Turns off SSL verification (it's on by default)."
    )
    return ap


def _livy_info_func(livy_url, batchId=None, state=None, **kwargs):
    """The runner func for the 'info' subcommand.

    Usage
    -----
    # Return info on all active jobs
    $ livy info

    # Return the state on all active jobs
    $ livy info --state

    # Return info on job #42
    $ livy info --batchId 42

    # Return the state of job #42
    $ livy info --batchId 42 --state
    """
    api_instance = livy_api.LivyAPI(server_url=livy_url, verify=kwargs['verify'])

    if batchId is not None:
        if state:
            # When you type this at the command line: `livy info {batchId} --state`
            _, resp = api_instance.state(batchId)
        else:
            # When you type this at the command line: `livy info {batchId}`
            resp = api_instance.info(batchId)
    else:
        # When you type this at the command line: `livy info`
        _, _, resp = api_instance.all_info(batchId)
        if state:
            # When you type this at the command line: `livy info --state`
            resp = {id: batch.state for id, batch in resp.items()}

    logger.info(resp)


def _livy_info_parser(subparsers) -> ArgumentParser:
    """
    Configure the `livy info` subparser
    """
    ap = subparsers.add_parser(
        "info", help="Parser for getting info on an active Livy job"
    )
    ap.set_defaults(func=_livy_info_func)
    ap.add_argument(
        "--state",
        action="store_true",
        default=False,
        help="Only show the current status of the job",
    )
    ap.add_argument(
        "batchId",
        action="store",
        nargs="?",
        default=None,
        help="The Livy batch ID that you want information for",
    )



def _livy_submit_func(
    livy_url: str,
    namenode_url: str,
    file: str,
    name: str = None,
    driverMemory: str = None,
    driverCores: int = None,
    executorMemory: str = None,
    executorCores: int = None,
    numExecutors: int = None,
    archives: List[str] = None,
    conda_env: str = None,
    queue: str = None,
    conf: Dict = None,
    args: List[str] = None,
    pyFiles: List[str] = None,
    **kwargs
):
    if conf is None:
        conf = {}
    conf.update({"spark.logConf": True})
    if args is not None:
        args = shlex.split(args)

    if archives is None:
        archives = []

    logger.debug("Value of args parameter:\n%s", pformat(args))

    # upload file to hdfs
    hdfs_file_path = hdfs_api.upload(namenode_url=namenode_url, local_file=file)
    # upload archives to hdfs
    hdfs_dirname = dirname(hdfs_file_path)

    hdfs_archives = []

    if conda_env is not None:
        # There's enough special case handling here that it's worth keeping this conda_env code separate from the
        # generic archives uploading
        if archives is None:
            archives = []
        res = urlparse(conda_env)
        path = res.path
        pythonroot = os.path.basename(path)
        # See if we have a symlink here. It'll be in the "fragment" portion of the urlparse result
        symlink = res.fragment
        if symlink:
            # the user has included a symlink. Yarn will unpack the archive to this path
            # inside of their yarn container
            pythonroot = symlink
        # spark.pyspark.driver.python defaults to spark.pyspark.python
        pythonpath = './%s/bin/python' % pythonroot
        conf['spark.pyspark.python'] = pythonpath
        conf['spark.yarn.appMasterEnv.PYSPARK_PYTHON'] = pythonpath

        if res.scheme.lower() in ('hdfs', 's3'):
            # We don't need to do anything here. Yarn will handle this properly
            hdfs_archives.append(conda_env)
        elif res.scheme == '':
            # this is a local path so we need to upload it, but we can reuse the archives handling code below
            archives.append(conda_env)
        else:
            err_msg = ("Your conda env arg is: %s\n"
                       "livy-submit works with hdfs, s3 or local file systems.\n"
                       "Using urlparse, it was determined that your env is at the following scheme: %s" %
                       (conda_env, res.scheme))
            raise RuntimeError(err_msg)

    # Upload all local archives to HDFS
    if archives is not None:
        for archive in archives:
            res = urlparse(archive)
            if res.scheme.lower() in ('hdfs', 's3'):
                # We don't need to do anything else here. Yarn will handle this properly
                hdfs_archives.append('conda_env')
                continue
            elif res.scheme != '':
                err_msg = ("Your conda env arg is: %s\n"
                           "livy-submit works with hdfs, s3 or local file systems.\n"
                           "Using urlparse, it was determined that your env is at the following scheme: %s" %
                           (archive, res.scheme))
                raise RuntimeError(err_msg)

            # Now we know the archive is local and we need to upload it to hdfs. But first we need to figure out
            # if the user has given us a Hadoop symlinking syntax, i.e., '/home/edill/foo.tar.gz#CONDA'
            symlink = None

            try:
                archive, symlink = archive.split("#")
            except:
                logger.debug("No symlink component found in %s" % archive)
                pass
            archive_path = hdfs_api.upload(
                namenode_url=namenode_url, local_file=archive, hdfs_dir=hdfs_dirname
            )
            if symlink:
                # Use the Spark/Yarn-specific way to create symlinks, if the user is specifying one
                archive_path = "%s#%s" % (archive_path, symlink)
            hdfs_archives.append("hdfs://%s" % archive_path)
        archives = hdfs_archives


    if pyFiles is not None:
        hdfs_pyfiles = []
        for pyfile in pyFiles:
            pyfile_path = hdfs_api.upload(
                namenode_url=namenode_url, local_file=pyfile, hdfs_dir=hdfs_dirname
            )
            hdfs_pyfiles.append("hdfs://%s" % pyfile_path)
        pyFiles = hdfs_pyfiles

    # Show the final conf
    logger.debug("conf:\n%s", pformat(conf))

    # format args to pass to the livy submit API
    submit_args = {
        "name": name,
        "file": hdfs_file_path,
        "driverMemory": driverMemory,
        "driverCores": driverCores,
        "executorMemory": executorMemory,
        "executorCores": executorCores,
        "numExecutors": numExecutors,
        "archives": archives,
        "queue": queue,
        "conf": conf,
        "args": args,
        "pyFiles": pyFiles,
    }
    # submit livy job
    api_instance = livy_api.LivyAPI(server_url=livy_url, verify=kwargs['verify'])
    batch = api_instance.submit(**submit_args)

    # Log livy job into to the console
    logger.info("Batch job submitted to Livy API:\n%s", pformat(batch))


def _livy_submit_parser(subparsers):
    """
    Configure the `livy submit` subparser
    """

    ap = subparsers.add_parser(
        "submit", help="Parser for submitting a job to the Livy /batches endpoint"
    )
    ap.set_defaults(func=_livy_submit_func)
    ap.add_argument(
        "--name",
        action="store",
        required=False,
        help="The name that your Spark job should have on the Yarn RM",
    )
    ap.add_argument(
        "--file",
        action="store",
        required=True,
        help=(
            "The file that should be executed during your Spark job. "
            "If this file is local it will be uploaded to a temporary "
            "path on HDFS so it will be accessible from your Spark "
            "driver and executors"
        ),
    )
    ap.add_argument(
        '--conda-env',
        help=("A conda environment archived with conda-pack to be used in this session. Functionally equivalent"
              "to adding it as an archive and setting some yarn / spark parameters. TODO: Note specifically"
              "what parameters are going to be set")
    )
    ap.add_argument(
        "--archives",
        action="append",
        help=(
            "An archive to be used in this session. Parameter can be used multiple "
            "times to provide multiple archives. Same deal as the `file` parameter. "
            "These archives will be uploaded to HDFS."
        ),
    )
    ap.add_argument(
        "--driver-memory",
        dest="driverMemory",
        action="store",
        help=(
            "e.g. 512m, 2g. Amount of memory to use for the driver process, i.e. "
            "where SparkContext is initialized, in the same format "
            "as JVM memory strings with a size unit suffix "
            '("k", "m", "g" or "t"). Overrides settings contained in config files.'
        ),
    )
    ap.add_argument(
        "--driver-cores",
        dest="driverCores",
        action="store",
        type=int,
        help=(
            "Number of cores to use for the driver process, only in cluster mode. "
            "Overrides settings contained in config files."
        ),
    )
    ap.add_argument(
        "--executor-memory",
        dest="executorMemory",
        action="store",
        help=(
            "e.g. 512m, 2g. Amount of memory to use per executor process, in "
            "the same format as JVM memory strings with a size unit suffix "
            '("k", "m", "g" or "t"). Overrides settings contained in config files.'
        ),
    )
    ap.add_argument(
        "--executor-cores",
        dest="executorCores",
        action="store",
        type=int,
        help=(
            "The number of cores for each executor. Overrides settings contained "
            "in config files."
        ),
    )
    ap.add_argument(
        "--num-executors",
        dest="numExecutors",
        action="store",
        type=int,
        help=("Number of executors to launch for this session"),
    )
    ap.add_argument(
        "--queue", action="store", help="The YARN queue that your job should run in"
    )
    ap.add_argument(
        "--args",
        action="store",
        help=(
            "Extra command line args for the application. If your python "
            "main is expecting command line args, use this variable to pass "
            "them in as space delimited. Will use shlex to split args"
        ),
    )
    ap.add_argument(
        "--py-files",
        dest="pyFiles",
        action="append",
        help=(
            "Python files to be used in this session. Parameter can be used multiple "
            "times to provide multiple archives. Same deal as the `file` parameter. "
            "These archives will be uploaded to HDFS."
        ),
    )
    ap.add_argument(
        "--conf",
        action="append",
        help=(
            "Additional Spark/Yarn/PySpark configuration properties. Parameter can be "
            "used multiple times to provide multiple parameters. Values should take the "
            "form of --conf spark.property=value"
        ),
    )


def _livy_kill_func(livy_url: str, batchId: int, **kwargs):
    """
    Terminate a Livy job and delete its state from the Livy server
    """
    api_instance = livy_api.LivyAPI(server_url=livy_url, verify=kwargs['verify'])
    resp = api_instance.kill(batchId)
    logger.info("Job killed:\n%s", pformat(resp))


def _livy_kill_parser(subparsers):
    """
    Configure the `livy kill` subparser
    """
    ap = subparsers.add_parser(
        "kill",
        help=(
            "Parser for killing a job that was submitted to the Livy /batches. "
            "Note that this also deletes the job info from the Livy server."
        ),
    )
    ap.set_defaults(func=_livy_kill_func)
    ap.add_argument("batchId", help="The Livy batch ID that you want to terminate")


def _livy_log_func(livy_url: str, batchId: int, follow: bool, **kwargs):
    """
    Implement the `log` and `log -f` functionality
    """
    print("value of follow: %s" % follow)
    api_instance = livy_api.LivyAPI(server_url=livy_url, verify=kwargs['verify'])

    # Get the current logs. We need to do this in either case
    if not follow:
        _, offset, num, stdout, stderr = api_instance.log(batchId)
        logger.info("stdout logs")
        logger.info("\n".join(stdout))
        logger.info("stderr logs")
        logger.info("\n".join(stderr))

        # Early exit if we are just looking for the logs once
        return

    # Start by showing all available logs
    prev_num = 0
    # Give a fake state so we go through the loop at least once.
    # Really, I want a do-while loop, but Python doesn't have that paradigm
    state = "starting"
    total_new_lines = 0
    total_lines = 0
    known_lines = set()
    while state in ("running", "starting"):
        _, state = api_instance.state(batchId)
        _, offset, num, stdout, stderr = api_instance.log(batchId)

        total_lines += num

        # This is inefficient, but it's also really really easy
        for line in stdout:
            if line not in known_lines:
                known_lines.add(line)
                logger.info(line)

        # Delay a little bit so we're not hammering the Livy server
        time.sleep(1)

    logger.debug("total lines received from Livy API: %s" % total_lines)
    logger.debug("total lines logged: %s" % len(known_lines))
    logger.debug("number of log lines from the API: %s" % num)


def _livy_log_parser(subparsers):
    """
    Configure the `livy log` and `livy log -f` parser
    """
    ap = subparsers.add_parser(
        "log",
        help=(
            "Parser for killing a job that was submitted to the Livy /batches. "
            "Note that this also deletes the job info from the Livy server."
        ),
    )
    ap.set_defaults(func=_livy_log_func)
    ap.add_argument("batchId", help="The Livy batch ID that you want to terminate")
    ap.add_argument(
        "-f",
        "--follow",
        default=False,
        action="store_true",
        help=(
            "Regularly poll Livy, fetch the most recent log lines and "
            "print them to the terminal"
        ),
    )


def _make_parser() -> ArgumentParser:
    base = _base_parser()
    subparsers = base.add_subparsers(help="sub-command help")
    _livy_info_parser(subparsers)
    _livy_submit_parser(subparsers)
    _livy_kill_parser(subparsers)
    _livy_log_parser(subparsers)
    return base


def cli():
    ap = _make_parser()

    args = ap.parse_args()

    loglevel = logging.INFO
    if args.verbose:
        loglevel = logging.DEBUG
    if args.quiet:
        loglevel = logging.WARNING

    _init_logger(loglevel)

    # set the pdb_hook as the except hook for all exceptions
    if args.pdb:

        def pdb_hook(exctype, value, traceback):
            pdb.post_mortem(traceback)

        sys.excepthook = pdb_hook

    # Convert args Namespace object into a dictionary for easier manipulation
    args_dict = {k: v for k, v in vars(args).items() if v is not None}
    cli_conf_dict = {}
    for cli_conf in args_dict.pop("conf", []):
        k, v = cli_conf.split("=", maxsplit=1)
        cli_conf_dict[k] = v
    #     cli_conf = {k: v for _ in args_dict.pop('conf', []) for k, v in _.split('=', maxsplit=1)}

    # Trim args we've already used
    del args_dict["verbose"]
    del args_dict["pdb"]
    logger.debug("cli args: %s", pformat(args_dict))

    # Get the sparkmagic configuration from its file
    sparkmagic_config = _sparkmagic_config(args_dict.pop("sparkmagic_config"))
    logger.debug("sparkmagic_config: %s", pformat(sparkmagic_config))

    # Get the Livy configuration from its file
    livy_submit_config = _livy_submit_config(args_dict.pop("livy_submit_config"))
    logger.debug("livy_submit_config: %s", pformat(livy_submit_config))

    # Create a single, unified set of config parameters with the priority in
    # increasing order being: sparkmagic config, livy submit config, command line args
    cfg = {}
    cfg.update(sparkmagic_config)
    logger.debug("config after adding sparkmagic_config:\n%s", pformat(cfg))
    for k, v in livy_submit_config.items():
        if k not in cfg:
            cfg[k] = v
        else:
            if isinstance(v, dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    #     cfg.update(livy_submit_config)
    logger.debug("config after adding livy_submit_config:\n%s", pformat(cfg))
    cfg.update(args_dict)
    logger.debug("config after adding CLI args:\n%s", pformat(cfg))

    cfg.get("conf", {}).update(cli_conf_dict)

    # Do the kinit before we run the subcommand
    # TODO: Implement this after we finalize the AE 5.2.3 secrets syntax

    # Run the specific function for each subcommand
    cfg["func"](**cfg)
