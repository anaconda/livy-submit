from contextlib import contextmanager
import sys
import pytest
import copy
import shlex
import logging
from livy_submit import cli
from os.path import join
import time
import json
from livy_submit.livy_api import Batch


@contextmanager
def sysargv(new_args):
    if isinstance(new_args, str):
        new_args = shlex.split(new_args)
    old_argv = copy.copy(sys.argv)
    sys.argv = new_args
    yield
    sys.argv = old_argv


def _parse_output_for_batch_object(output):
    output_list = [line for line in output.split("\n") if line]
    batch_output = output_list[-1]
    batch = eval(batch_output)
    assert isinstance(batch, Batch), "Eval'd object should result in a Batch object"
    return batch


@pytest.fixture(scope="session")
def base_cmd(livy_submit_config_file, sparkmagic_config_file):
    base_cmd = [
        "livy",
        "--sparkmagic-config",
        sparkmagic_config_file,
        "--livy-submit-config",
        livy_submit_config_file,
    ]
    return " ".join(base_cmd)


@pytest.fixture(scope='session')
def info_cmd(base_cmd):
    return base_cmd + ' info'


@pytest.fixture(scope="function")
def job_submit_cmd(pi_file, livy_submit_config_file, base_cmd):
    submit_cmd = (
        base_cmd
        + " "
        + " ".join(
            [
                "submit",
                "--name",
        	"test-cli_submit-from-pytest-%s" % time.time(),
                "--file",
                pi_file,
                "--args",
                "'100'",
            ]
        )
    )
    print(submit_cmd)
    return submit_cmd


@pytest.fixture(scope="function")
def job_submit_cmd_noname(pi_file, livy_submit_config_file, base_cmd):
    submit_cmd = (
        base_cmd
        + " "
        + " ".join(
            [
                "submit",
                "--file",
                pi_file,
                "--args",
                "'100'",
            ]
        )
    )
    print(submit_cmd)
    return submit_cmd


@pytest.mark.parametrize('job_submit_cmd', [job_submit_cmd, job_submit_cmd_noname])
@pytest.fixture(scope="function")
def submitted_job(kinit, pi_file, capsys, job_submit_cmd, info_cmd):
    with sysargv(job_submit_cmd):
        cli.cli()

    # out does not contain much interesting stuff. Python sends logging messages to `err`
    # by default :(
    out, err = capsys.readouterr()
    
    # Utilize `livy info  {batchId} --state` functionality to get the batch info
    batch_job1 = _parse_output_for_batch_object(err)
    
    tries = 0
    info_cmd = '%s %d' % (info_cmd, batch_job1.id)
    while batch_job1.appId is None and tries < 10:
        time.sleep(2)
        tries += 1
        with sysargv(info_cmd):
            cli.cli()
        out, err = capsys.readouterr()
        batch_job1 = _parse_output_for_batch_object(err)
        
    yield batch_job1


@pytest.fixture(scope="function")
def finished_job(submitted_job, capsys, base_cmd):
    job_state = submitted_job.state
    job_id = submitted_job.id

    # Clear the output
    out, err = capsys.readouterr()

    while job_state in ("starting", "running"):
        time.sleep(1)
        cmd = f"{base_cmd} info {job_id} --state"
        with sysargv(cmd):
            cli.cli()
        out, err = capsys.readouterr()
        err = err.strip("\n")
        job_state = err

    # Make sure we finish the job successfully
    assert job_state == "success", "Job should be finished successfully"

    yield submitted_job


def test_cli(submitted_job, finished_job, capsys, base_cmd):
    # Test the `livy info` functionality
    cmd = f"{base_cmd} info"
    with sysargv(cmd):
        cli.cli()

    out, err = capsys.readouterr()

    # The output here is literally a python dict dumped to the screen, so we
    # should be able to re-materialize it as a python dict and check to see if
    # the keys are in there
    err = eval(err.strip("\n"))
    assert submitted_job.id in err, "The submitted job should be in the output"
    assert finished_job.id in err, "The finished job should be in the output"

    # Test the `livy info --state` functionality
    cmd = f"{base_cmd} info --state"
    with sysargv(cmd):
        cli.cli()

    out, err = capsys.readouterr()

    # The output here is literally a python dict dumped to the screen, so we
    # should be able to re-materialize it as a python dict and check to see if
    # the keys are in there
    err = eval(err.strip("\n"))
    assert submitted_job.id in err, "The submitted job should be in the output"
    assert finished_job.id in err, "The finished job should be in the output"

    
def test_info_func(submitted_job, capsys, base_cmd):
    cmd = f"{base_cmd} info {submitted_job.id}"
    with sysargv(cmd):
        cli.cli()
    
    out, err = capsys.readouterr()
    
    assert str(submitted_job.id) in err, "The submitted job ID should appear in the output"
        

@pytest.fixture()
def config_missing_kernel(sparkmagic_config_file, tmpdir):
    with open(sparkmagic_config_file, "r") as f:
        contents = json.loads(f.read())

    del contents["kernel_python_credentials"]

    config_file_path = join(tmpdir, "config.json")

    with open(config_file_path, "w") as f:
        f.write(json.dumps(contents))

    yield config_file_path


def test_sparkmagic_missing_python_kernel(config_missing_kernel, capsys):
    """There are two error conditions in the code for reading the sparkmagic config.
    This test exercises the error condition where the sparkmagic config file is missing
    the entry for the Python kernel.
    """
    cli._init_logger(logging.INFO)
    cfg = cli._sparkmagic_config(config_missing_kernel)
    assert (
        cfg["livy_url"] is None and cfg["livy_port"] is None
    ), "The livy info should be set to None since it is not present in the provided sparkmagic kernel"
    out, err = capsys.readouterr()
    assert f"kernel_python_credentials' not found in sparkmagic configuration ({config_missing_kernel})", "The user should be notified that their Livy server cannot be found in the sparkmagic configuration and shown the path to the offending sparkmagic config"


def test_sparkmagic_config_bad_path(capsys):
    """There are two error conditions in the code for reading the sparkmagic config.
    This test exercises the error condition where the sparkmagic config file cannot be found.
    """
    cli._init_logger(logging.INFO)
    bad_sparkmagic_path = "/this/does/not/exist"
    cfg = cli._sparkmagic_config(bad_sparkmagic_path)
    assert cfg == {}, "Config should be an empty dict if we don't find the path"
    out, err = capsys.readouterr()
    assert (
        bad_sparkmagic_path in err
    ), "The user should be notified that the sparkmagic path cannot be found"
    assert (
        "Cannot load sparkmagic defaults" in err
    ), "The user should be notified when the sparkmagic configuration cannot be found"


def test_cli_log(finished_job, capsys, base_cmd):
    cmd = f"{base_cmd} log {finished_job.id}"
    with sysargv(cmd):
        cli.cli()
    out, err = capsys.readouterr()
    #assert "Pi is roughly 3.1" in err, "The value of Pi should be in the logs"
    assert finished_job.appId in err, "The spark application ID should be in the logs"


def test_cli_log_follow(submitted_job, capsys, base_cmd):
    cmd = f"{base_cmd} log {submitted_job.id} -f"
    with sysargv(cmd):
        cli.cli()
    out, err = capsys.readouterr()
    #assert "Pi is roughly 3.1" in err, "The value of Pi should be in the logs"
    assert submitted_job.appId in err, "The spark application ID should be in the logs"

    cmd = f"{base_cmd} info {submitted_job.id} --state"
    with sysargv(cmd):
        cli.cli()
    out, err = capsys.readouterr()
    job_state = err.strip("\n")

    # Make sure we finish the job successfully
    assert job_state == "success", "Job should be finished successfully"
