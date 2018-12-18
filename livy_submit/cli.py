from argparse import ArgumentParser
from os.path import expanduser, dirname
from typing import Dict, List
from . import livy_api, hdfs_api
from pprint import pprint, pformat
import json
import os
import shlex
import sys
import pdb
import traceback

def _sparkmagic_config(config_path: str) -> Dict:
    """Read the sparkmagic configuration file for the 
    Spark defaults and the Livy server url/port
    
    Returns
    -------
    dict: Keys are "spark_config", "livy_url" and "livy_port"
    """
    if not os.path.exists(config_path):
        print('%s not found. Cannot load sparkmagic defaults' % config_path)
        return {}
    with open(config_path, 'r') as f:
        cfg = json.loads(f.read())
    spark_config = cfg.get('session_configs')
    livy_server = cfg.get('kernel_python_credentials', {}).get('url')
    if livy_server is not None:
        url, port = livy_server.rsplit(':', maxsplit=1)
    else:
        err_str = ("'kernel_python_credentials' not found in sparkmagic "
                   "configuration (%s). Unable to automatically determine "
                   "the location of your Livy server" % config_path)
        print(err_str)
        url, port = None, None

    return {'conf': spark_config, 'livy_url': url, 'livy_port': port}


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
    if not os.path.exists(config_path):
        print('%s not found. Cannot load livy submit defaults' % config_path)
        return {}
    with open(config_path, 'r') as f:
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
        default=expanduser("~/.sparkmagic/config.json"),
        help=("The location of the sparkmagic configuration file. "
              "Will extract the Livy url/port and the spark defaults "
              "set in `session_configs`")
    )
    ap.add_argument(
        "--livy-submit-config",
        action="store",
        default=expanduser("~/.livy-submit.json"),
        help="The location of the livy submit configuration file",
    )
    ap.add_argument(
        "--namenode-url",
        action="store",
        help=("The url of the namenode. Should include protocol "
              "(http/https) and port (50070)")
    )
    ap.add_argument(
        "--livy-url",
        action="store",
        help=("The url of the Livy server. Should include protocol "
              "(http/https) and port (8998)")
    )
    ap.add_argument(
        '--pdb',
        action="store_true",
        default=False,
        help="Drop into a debugger on client-side exception"
    )
    return ap


def _info_func(livy_url, batchId=None, state=None, **kwargs):
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
    api_instance = livy_api.LivyAPI(server_url=livy_url)
    if batchId is not None:
        if state:
            _, resp = api_instance.state(batchId)
        else:
            resp = api_instance.info(batchId)
    else:
        _, _, resp = api_instance.all_info(batchId)
        if state:
            resp = {id: batch.state for id, batch in resp.items()}
            
    pprint(resp)
    
    
def _livy_info(subparsers) -> ArgumentParser:
    """
    Configure the `livy info` subparser
    """
    ap = subparsers.add_parser(
        'info',
        help="Parser for getting info on an active Livy job"
    )
    ap.set_defaults(func=_info_func)
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


def _submit_func(
    livy_url: str,
    namenode_url: str,
    name: str,
    file: str,
    driverMemory: str = None,
    driverCores: int = None,
    executorMemory: str = None,
    executorCores: int = None,
    numExecutors: int = None,
    archives: List[str] = None,
    queue: str = None,
    conf: List[str] = None,
    args: List[str] = None,
    **kwargs
):
    print('conf:\n%s' % pformat(conf))
    
    if args is not None:
        args = shlex.split(args)
    
    print('args:\n%s' % pformat(args))
        
    
    # upload file to hdfs
    hdfs_file_path = hdfs_api.upload(namenode_url=namenode_url, local_file=file)
    hdfs_file_path = 'hdfs://%s' % hdfs_file_path
    # upload archives to hdfs
    hdfs_dirname = dirname(hdfs_file_path)
    if archives is not None:
        hdfs_archives = []
        for archive in archives:
            archive_path = hdfs_api.upload(
                namenode_url=namenode_url, 
                local_file=archive, 
                hdfs_dir=dfs_dir)
            hdfs_archives.append('hdfs://%s' % hdfs_archive_path)
        archives = hdfs_archives
    # format args to pass to the livy submit API
    submit_args = {
        'name': name,
        'file': hdfs_file_path,
        'driverMemory': driverMemory,
        'driverCores': driverCores,
        'executorMemory': executorMemory,
        'executorCores': executorCores,
        'numExecutors': numExecutors,
        'archives': archives,
        'queue': queue,
        'conf': conf,
        'args': args
    }
    # submit livy job
    api_instance = livy_api.LivyAPI(server_url=livy_url)
    batch = api_instance.submit(**submit_args)
    
    # Print livy job into to the console
    print("Batch job submitted to Livy API:")
    pprint(batch)

def _livy_submit(subparsers):
    """
    Configure the `livy submit` subparser
    """

    ap = subparsers.add_parser(
        'submit',
        help="Parser for submitting a job to the Livy /batches endpoint"
    )
    ap.set_defaults(func=_submit_func)
    ap.add_argument(
        '--name',
        action='store',
        required=True,
        help='The name that your Spark job should have on the Yarn RM'
    )
    ap.add_argument(
        '--file',
        action='store',
        required=True,
        help=("The file that should be executed during your Spark job. "
              "If this file is local it will be uploaded to a temporary "
              "path on HDFS so it will be accessible from your Spark "
              "driver and executors")
    )
    ap.add_argument(
        '--driver-memory',
        action='store',
        help=('e.g. 512m, 2g. Amount of memory to use for the driver process, i.e. '
              'where SparkContext is initialized, in the same format '
              'as JVM memory strings with a size unit suffix '
              '("k", "m", "g" or "t"). Overrides settings contained in config files.')
    )
    ap.add_argument(
        '--driverCores',
        action='store',
        help=('Number of cores to use for the driver process, only in cluster mode. '
              'Overrides settings contained in config files.')
    )
    ap.add_argument(
        '--executorMemory',
        action='store',
        help=('e.g. 512m, 2g. Amount of memory to use per executor process, in '
              'the same format as JVM memory strings with a size unit suffix '
              '("k", "m", "g" or "t"). Overrides settings contained in config files.')
    )
    ap.add_argument(
        '--executorCores',
        action='store',
        help=('The number of cores for each executor. Overrides settings contained '
              'in config files.')
    )
    ap.add_argument(
        '--archives',
        action='store',
        help=('An archive to be used in this session. Parameter can be used multiple '
              'times to provide multiple archives. Same deal as the `file` parameter. '
              'These archives will be uploaded to HDFS.')
    )
    ap.add_argument(
        '--queue',
        action='store',
        help='The YARN queue that your job should run in'
    )
    ap.add_argument(
        '--args',
        action='store',
        help=('Extra command line args for the application. If your python '
              'main is expecting command line args, use this variable to pass '
              'them in as space delimited. Will use shlex to split args')
    )
    


def _make_parser() -> ArgumentParser:
    base = _base_parser()
    subparsers = base.add_subparsers(help='sub-command help')
    _livy_info(subparsers)
    _livy_submit(subparsers)
    return base


def cli():
    print('cli 1')
    ap = _make_parser()
    print('cli 2')
    
    args = ap.parse_args()
    
    # set the pdb_hook as the except hook for all exceptions
    if args.pdb:
        def pdb_hook(exctype, value, traceback):
            pdb.post_mortem(traceback)
        sys.excepthook = pdb_hook
        
    # Convert args Namespace object into a dictionary for easier manipulation
    args_dict = {k: v for k, v in vars(args).items() if v is not None}
    print('cli args: %s' % pformat(args_dict))
    
        
    # Get the sparkmagic configuration from its file
    sparkmagic_config = _sparkmagic_config(args_dict.pop('sparkmagic_config'))
    print('sparkmagic_config: %s' % pformat(sparkmagic_config))
    
    # Get the Livy configuration from its file
    livy_submit_config = _livy_submit_config(args_dict.pop('livy_submit_config'))
    print('livy_submit_config: %s' % pformat(livy_submit_config))
    
    # Create a single, unified set of config parameters with the priority in 
    # increasing order being: sparkmagic config, livy submit config, command line args
    cfg = {}
    cfg.update(sparkmagic_config)
    print('config after adding sparkmagic_config:\n%s' % pformat(cfg))
    for k, v in livy_submit_config.items():
        if k not in cfg:
            cfg[k] = v
        else:
            if isinstance(v, (tuple, list)):
                cfg[k] += v
            elif isinstance(v, dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
#     cfg.update(livy_submit_config)
    print('config after adding livy_submit_config:\n%s' % pformat(cfg))
    cfg.update(args_dict)
    print('config after adding CLI args:\n%s' % pformat(cfg))
    print('cli 3')
    
    # Do the kinit before we run the subcommand
    # TODO: Implement this after we finalize the AE 5.2.3 secrets syntax
    
    # Run the specific function for each subcommand
    cfg['func'](**cfg)
    print('cli 4')
