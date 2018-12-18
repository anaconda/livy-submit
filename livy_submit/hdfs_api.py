import os
from hdfs.ext.kerberos import KerberosClient
import getpass
import uuid
import time


_NAMENODE_PORT = 50070


def get_client(namenode_url: str, namenode_port: int = None) -> KerberosClient:
    """Thin wrapper around KerberosClient
    
    Parameters
    ----------
    namenode_url: The url of the namenode
    namenode_port: (optional) The port that the namenode service is running on. Defaults to 
        value of module-level variable _NAMENODE_PORT.
    """
    port = namenode_port or _NAMENODE_PORT
    if not namenode_url.startswith('http'):
        namenode_url = 'http://%s' % namenode_url
    return KerberosClient('%s:%s' % (namenode_url, port))


def delete(hdfs_dir: str,
           namenode_url: str,
           namenode_port: int = None):
    """
    Delete `hdfs_dir` and all its contents
        
    Parameters
    ----------
    hdfs_dir: The folder (or full path) to upload `local_file` to. If none is provided,
        then the file will be uploaded to the following directory: 
        /tmp/livy_{current user name}_{First six characters of uuid4}_{current time in seconds}'
    namenode_url: The url of the namenode
    namenode_port: (optional) The port that the namenode service is running on. Defaults to 
        value of module-level variable _NAMENODE_PORT.
    """
    port = namenode_port or _NAMENODE_PORT
    client = get_client(namenode_url, port)
    client.delete(hdfs_dir, recursive=True)



def upload(local_file: str,
           namenode_url: str,
           namenode_port: int = None,
           hdfs_dir: str = None) -> str:
    """
    Parameters
    ----------
    local_file: The local file to upload
    namenode_url: The url of the namenode
    namenode_port: (optional) The port that the namenode service is running on. Defaults to 
        value of module-level variable _NAMENODE_PORT.
    hdfs_dir: (optional) The folder (or full path) to upload `local_file` to. If none is provided,
        then the file will be uploaded to the following directory: 
        /tmp/livy_{current user name}_{First six characters of uuid4}_{current time in seconds}'
        
    Returns
    -------
    str: The directory where `local_file` was uploaded so you can use it to upload the rest of your
        files needed for your Spark job
    """
    port = namenode_port or _NAMENODE_PORT
    client = get_client(namenode_url, port)
    
    if hdfs_dir is None:
        hdfs_dir = f'/tmp/livy_{getpass.getuser()}_{str(uuid.uuid4())[:6]}_{round(time.time())}/'
        
    client.makedirs(hdfs_dir)
    resp = client.upload(hdfs_dir, local_file, overwrite=True)
    return hdfs_dir
    
    
    
