import pexpect
from subprocess import check_output, check_call


def kinit_keytab(path, principal):
    """
    kinit using a keytab

    Parameters
    ----------
    path : str
        The path to the keytab
    principal : str
        The full principal that should be used to kinit
    """
    cmd = f"kinit -kt {path} {principal}"
    resp = check_call(cmd, shell=True)
    assert resp == 0, f"This command failed: {cmd}"
    print(check_output("klist").decode())


def kinit_username(username, password, domain=None):
    """
    Use pexpect to run kinit so we can access resources that
    require kerberos authentication

    Parameters
    ----------
    username : str
        The kerberos username. You may optionally include the domain along with
        your username if you need to specify a diferent domain than whatever
        is set as the `default_realm` in the krb5.conf
    password : str
        The password associated with `username`
    """
    p = pexpect.spawn("kinit %s" % username)
    p.sendline(password)
    p.expect(pexpect.EOF)
    p.close()

    if p.exitstatus == 0:
        print(check_output("klist").decode())
    else:
        output = p.before.decode()
        # Show the user the output of the failed kinit attempt, but
        # make sure to mask the token so we dont leak that into logs
        output = output.replace(password, "<<MASKED TOKEN>>")
        raise RuntimeError(
            "kinit unsuccessful. Here is the full output from the kinit attempt:\n%s"
            % output
        )
