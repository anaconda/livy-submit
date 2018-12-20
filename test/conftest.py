import pytest
import os
import pexpect
import subprocess


@pytest.fixture(scope="session")
def NAMENODE_URL():
    return "http://ip-172-31-20-241.ec2.internal:50070"


@pytest.fixture(scope="session")
def LIVY_URL():
    return "http://ip-172-31-20-241.ec2.internal:8998"


@pytest.fixture(scope="session")
def pi_file():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cur_dir, "data", "pi.py")


@pytest.fixture(scope="session")
def livy_submit_config_file():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cur_dir, "data", "livy-submit.json")


@pytest.fixture(scope="session")
def sparkmagic_config_file():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cur_dir, "data", "config.json")


@pytest.fixture(scope="session")
def livy_test_user_and_password():
    return "edill", "anaconda"


@pytest.fixture(scope="session")
def kinit(livy_test_user_and_password):
    """
    Use pexpect to run kinit so we can access resources that
    require kerberos authentication
    """
    username, password = livy_test_user_and_password
    p = pexpect.spawn("kinit %s" % username)
    p.sendline(password)
    p.expect(pexpect.EOF)
    p.close()

    if p.exitstatus == 0:
        print(subprocess.check_output("klist").decode())
    else:
        output = p.before.decode()
        raise RuntimeError(
            "kinit unsuccessful. Here is the full output from the kinit attempt:\n%s"
            % output
        )

    yield
    # Now handle cleanup after the test function is completed
    # Destroy the kerberos TGT for the username that we kinit'd as
    resp = subprocess.check_call("kdestroy")
