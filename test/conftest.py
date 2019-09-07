import pytest
import os
import pexpect
import subprocess


@pytest.fixture(scope="session")
def NAMENODE_URL():
    return "http://ec2-3-93-61-21.compute-1.amazonaws.com:50070"


@pytest.fixture(scope="session")
def LIVY_URL():
    return "http://livy.demo.anaconda.com:8998"


@pytest.fixture(scope="session")
def pi_file():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cur_dir, "data", "pi.py")


@pytest.fixture(scope="session")
def livy_submit_config_file():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cur_dir, "data", "livy-submit.json")


@pytest.fixture(scope='session')
def pi_runner_archive(tmpdir):
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    pi_runner_file = os.path.join(cur_dir, "data", "pi_runner.py")
    zip_file_path = join(tmpdir, 'pi_runner.zip')
    with zipfile.ZipFile(zip_file_path, 'w') as zf:
        zf.write(pi_runner_file)
    
    return zip_file_path
    

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
