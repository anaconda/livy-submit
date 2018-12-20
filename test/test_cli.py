from contextlib import contextmanager
import sys
import pytest
import copy
import shlex
from livy_submit import cli
import time
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
    output_list = [line for line in output.split('\n') if line]
    batch_output = output_list[-1]
    batch = eval(batch_output)
    assert isinstance(batch, Batch), "Eval'd object should result in a Batch object"
    return batch
    
    
@pytest.fixture(scope='session')
def base_cmd(livy_submit_config_file, sparkmagic_config_file):
    base_cmd = [
        "livy", 
        "--sparkmagic-config", sparkmagic_config_file,
        "--livy-submit-config", livy_submit_config_file,
    ]
    return ' '.join(base_cmd)


@pytest.fixture(scope='session')
def job_submit_cmd(pi_file, livy_submit_config_file, base_cmd):
    submit_cmd = base_cmd + ' ' + ' '.join([
        "submit",
        "--name", "test_cli_submit",
        '--file', pi_file])
    print(submit_cmd)
    return submit_cmd
    
    
@pytest.fixture(scope='function')
def submitted_job(kinit, pi_file, capsys, job_submit_cmd):
    with sysargv(job_submit_cmd):
        cli.cli()
        
    # out does not contain much interesting stuff. Python sends logging messages to `err`
    # by default :(
    out, err = capsys.readouterr()
    
    # Utilize `livy info  {batchId} --state` functionality to get the batch info
    batch_job1 = _parse_output_for_batch_object(err)
    
    yield batch_job1
    

@pytest.fixture(scope='function')
def finished_job(submitted_job, capsys, base_cmd):
    job_state = submitted_job.state
    job_id = submitted_job.id
    
    # Clear the output
    out, err = capsys.readouterr()
    
    while job_state in ('starting', 'running'):
        time.sleep(1)
        cmd = f"{base_cmd} info {job_id} --state"
        with sysargv(cmd):
            cli.cli()
        out, err = capsys.readouterr()
        err = err.strip("\n")
        job_state = err
    
    # Make sure we finish the job successfully
    assert job_state == 'success', "Job should be finished successfully"
    
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
    err = eval(err.strip('\n'))
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
    err = eval(err.strip('\n'))
    assert submitted_job.id in err, "The submitted job should be in the output"
    assert finished_job.id in err, "The finished job should be in the output"
    
    
    
        
        

def test_cli_log():
    pass

def test_cli_log_follow():
    pass

