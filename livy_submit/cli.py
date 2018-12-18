from argparse import ArgumentParser
from os.path import expanduser
from typing import Dict
from . import livy_api
from pprint import pprint
import json
import os

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

    return {'spark_config': spark_config, 'livy_url': url, 'livy_port': port}


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
        default=expanduser("~/.livy-submit"),
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
    return ap


def _info_func(livy_url, batchId=None, state=None, **kwargs):
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
    
    
def _livy_info(subparsers):
    """Configure the `livy info` subparser
    
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
    
    Parameters
    ----------
    base_parser

    Returns
    -------
    ArgumentParser
    """

    ap = subparsers.add_parser(
        'info',
        help="Parser for getting info on an active Livy job"
    )
    ap.add_argument(
        "--state",
        action="store_true",
        default=False,
        help="Only show the current status of the job",
    )
    ap.add_argument(
        "--batchId",
        action="store",
        help="The Livy batch ID that you want information for",
    )
    ap.set_defaults(func=_info_func)


def _make_parser() -> ArgumentParser:
    base = _base_parser()
    subparsers = base.add_subparsers(help='sub-command help')
    _livy_info(subparsers)
    return base


def cli():
    print('cli 1')
    ap = _make_parser()
    print('cli 2')
    args = ap.parse_args()
    args_dict = {k: v for k, v in args._get_kwargs() if v is not None}
    print('cli args: %s' % args_dict)
    sparkmagic_config = _sparkmagic_config(args.sparkmagic_config)
    print('sparkmagic_config: %s' % sparkmagic_config)
    livy_submit_config = _livy_submit_config(args.livy_submit_config)
    print('livy_submit_config: %s' % livy_submit_config)
    cfg = {}
    cfg.update(sparkmagic_config)
    cfg.update(livy_submit_config)
    cfg.update(args_dict)
    print('cfg: %s' % cfg)
    print('cli 3')
    args.func(**cfg)
    print('cli 4')
