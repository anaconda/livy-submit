from livy_submit import hdfs_api
import pytest
import os


@pytest.fixture(scope='session')
def NAMENODE_URL():
    return 'ip-172-31-20-241.ec2.internal'


@pytest.fixture(scope='session')
def pi_file():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cur_dir, 'data', 'pi.py')
