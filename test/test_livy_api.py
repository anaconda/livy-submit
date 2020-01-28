import time
import pytest
from livy_submit import livy_api, hdfs_api
import hdfs
from os.path import dirname


@pytest.fixture(scope="session")
def api_instance(LIVY_URL):
    return livy_api.LivyAPI(server_url=LIVY_URL)


@pytest.yield_fixture(scope="session")
def upload_pi_file(NAMENODE_URL, pi_file, kinit):
    client = hdfs_api.get_client(NAMENODE_URL)
    hdfs_filepath = hdfs_api.upload(namenode_url=NAMENODE_URL, local_file=pi_file)
    hdfs_dirname = dirname(hdfs_filepath)
    resp = client.list(hdfs_dirname)
    assert "pi.py" in resp

    yield "hdfs://%s" % hdfs_filepath
    # Now handle cleanup after the test function is completed
    hdfs_api.delete(hdfs_dir=hdfs_dirname, namenode_url=NAMENODE_URL)
    with pytest.raises(hdfs.util.HdfsError):
        client.list(hdfs_dirname)


def test_end_to_end(api_instance, kinit, upload_pi_file, livy_test_user_and_password):
    """Given an uploaded pi.py executable, a valid Kerberos TGT and
    an active python Livy API instance, when we submit a job to the
    Livy server via the Python API, we should be able to query its status
    via the API methods
    """
    print(upload_pi_file)
    job1 = api_instance.submit(
        name="test-job-from-pytest-%s" % time.time(), file=upload_pi_file
    )
    print(job1)

    # Make sure that the info function is returning the correct job
    resp = api_instance.info(job1.id)
    assert resp == job1

    # Make sure that our job is in the all_info output
    start, total, batches = api_instance.all_info()
    assert resp.id in batches

    # Make sure that our state is running or starting
    batch_id, state = api_instance.state(job1.id)
    assert state in ("starting", "running")
    assert batch_id == job1.id

    # Wait until we are finished, but don't wait more than one minute
    tstart = time.time()
    while state in ("starting", "running"):
        if time.time() - tstart > 60:
            batch_id, offset, total, stdout, stderr = api_instance.log(job1.id)
            print("\n".join(stdout))
            api_instance.kill(job1.id)
            raise RuntimeError(
                "Job did not finish in 60 seconds. is your Yarn queue full?"
            )
        time.sleep(0.5)
        batch_id, state = api_instance.state(job1.id)

    # Make sure that the value of PI is in the output
    batch_id, offset, total, stdout, stderr = api_instance.log(job1.id)
    logstring = "\n".join(stdout)
    # If the following test fails, then you'll need to run this:
    # hdfs dfs -mkdir /user/livy-submit-test
    # hdfs dfs -chown livy-submit-test:livy-submit-test /user/livy-submit-test
    assert (
        'Permission denied: user=livy-submit-test, access=WRITE, inode="/user":hdfs:hadoop:drwxr-xr-x'
        not in logstring
    ), "You need to make a home directory on hdfs for the user listed in the `livy_test_user_and_password` fixture in this .py file and make sure that it is owned by that user"

    username, password = livy_test_user_and_password
    assert "User %s not found" % username not in logstring, (
        "You need to make sure that your test user (%s, in this case) exists on all of the nodes in your hadoop cluster. Alternatively, modify this source file to use your personal kerberos account for this test"
        % username
    )

    print(logstring)

    #assert "Pi is roughly 3.1" in logstring

    #print(logstring)


def test_unique_job_name(api_instance, kinit, upload_pi_file, livy_test_user_and_password):
    name="test-job-from-pytest-%s" % time.time()
    job1 = api_instance.submit(name=name, file=upload_pi_file)

    with pytest.raises(ValueError):
        job2 = api_instance.submit(name=name, file=upload_pi_file)


def test_all_info(api_instance, kinit):
    resp = api_instance.all_info()


def test_kill(api_instance, kinit, upload_pi_file, livy_test_user_and_password):
    """Given an uploaded pi.py executable, a valid Kerberos TGT and
    an active python Livy API instance, when we submit a job to the
    Livy server via the Python API, we should be able to query its status
    via the API methods
    """
    print(upload_pi_file)
    job1 = api_instance.submit(
        name="test-job-from-pytest-%s" % time.time(), file=upload_pi_file
    )
    print(job1)

    ret = api_instance.kill(job1.id)

    print(ret)
