from livy_submit import hdfs_api
import pytest
from hdfs.util import HdfsError
from os.path import dirname


def test_upload_and_delete(NAMENODE_URL, pi_file, kinit):
    client = hdfs_api.get_client(NAMENODE_URL)
    hdfs_filepath = hdfs_api.upload(namenode_url=NAMENODE_URL, local_file=pi_file)
    hdfs_dirname = dirname(hdfs_filepath)
    resp = client.list(hdfs_dirname)
    assert "pi.py" in resp
    hdfs_api.delete(hdfs_dir=hdfs_dirname, namenode_url=NAMENODE_URL)
    with pytest.raises(HdfsError):
        client.list(hdfs_dirname)
