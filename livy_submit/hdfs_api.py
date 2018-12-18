from hdfs.ext.kerberos import KerberosClient
import getpass
import uuid
import time


def get_client(namenode_url: str) -> KerberosClient:
    """Thin wrapper around KerberosClient

    Parameters
    ----------
    namenode_url: The url of the namenode. Should include protocol (http/https) and port
    """
    return KerberosClient(namenode_url)


def delete(hdfs_dir: str, namenode_url: str):
    """
    Delete `hdfs_dir` and all its contents

    Parameters
    ----------
    hdfs_dir: The folder (or full path) to upload `local_file` to. If none is provided,
        then the file will be uploaded to the following directory:
        /tmp/livy_{current user name}_{First six characters of uuid4}_{current time in seconds}'
    namenode_url: The url of the namenode. Should include protocol (http/https) and port
    """
    client = get_client(namenode_url)
    client.delete(hdfs_dir, recursive=True)


def upload(
    local_file: str,
    namenode_url: str,
    hdfs_dir: str = None,
) -> str:
    """
    Parameters
    ----------
    local_file: The local file to upload
    namenode_url: The url of the namenode. Should include protocol (http/https) and port
    hdfs_dir: (optional) The folder (or full path) to upload `local_file` to. If none is provided,
        then the file will be uploaded to the following directory:
        /tmp/livy_{current user name}_{First six characters of uuid4}_{current time in seconds}'

    Returns
    -------
    str: The directory where `local_file` was uploaded so you can use it to upload the rest of your
        files needed for your Spark job
    """
    client = get_client(namenode_url)

    if hdfs_dir is None:
        hdfs_dir = f"/tmp/livy_{getpass.getuser()}_{str(uuid.uuid4())[:6]}_{round(time.time())}/"

    client.makedirs(hdfs_dir)
    resp = client.upload(hdfs_dir, local_file, overwrite=True)
    print("file uploaded to %s" % resp)
    return hdfs_dir
