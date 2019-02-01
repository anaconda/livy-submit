from hdfs.ext.kerberos import KerberosClient
import getpass
import uuid
import time
import subprocess


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


def get_kerberos_user() -> str:
    """Get the kerberos user from klist -e.

    svc_user/my.hostname.com@MY.DOMAIN.COM -> svc_user
    svc_user@MY.DOMAIN.COM -> svc_user

    """
    unix_cmd = (
        'klist -e | grep Default | cut -d " " -f 3 | cut -d "@" -f 1 | cut -d "/" -f 1'
    )
    return subprocess.check_output(unix_cmd, shell=True).decode().strip()


def upload(local_file: str, namenode_url: str, hdfs_dir: str = None) -> str:
    """
    Parameters
    ----------
    local_file: The local file to upload
    namenode_url: The url of the namenode. Should include protocol (http/https) and port
    hdfs_dir: (optional) The directory in which to place `local_file`. If not provided, an hdfs
        directory will be generated:
        /user/{hdfs_user}/livy-submit-files/{first six chars of uuid4}_{current time in seconds}'

    Returns
    -------
    str: The full path to `local_file` was uploaded.
    """
    client = get_client(namenode_url)
    if hdfs_dir is None:
        hdfs_user = get_kerberos_user()
        hdfs_dir = f"/user/{hdfs_user}/livy-submit-files/{str(uuid.uuid4())[:6]}_{round(time.time())}/"

    client.makedirs(hdfs_dir)
    hdfs_path = client.upload(hdfs_dir, local_file, overwrite=True)
    print("file uploaded to %s" % hdfs_path)
    return hdfs_path
