# livy-submit as REST API

We can use livy-submit inside of a REST API by using the following libraries:

```
- tranquilizer
- livy-submit
```

We will need to do the following:

1. Define url for namenode and livy server
2. Define auth function that takes a username/password or a Kerberos keytab
3. Define function we want to execute as the REST endpoint. The livy-submit call lives inside of this function
4. Publish the REST endpoint using tranquilizer

## 1. Define url for namenode and livy server

These are pretty straightforward. 
Ideally this info could be pulled from the default livy configuration path, but there's no "load_config" function yet in livy-submit, so we will need to define it manually for now.

```python
namenode = "http://your.namenode.company.com:50070"
livy = "http://your.livy.company.com:8998"
```

## 2. Define auth function

Let's assume that you've got access to a keytab. 
If you're using Anaconda Enterprise then you can upload a keytab to your project. 
Then you can use that keytab inside of this REST API deployment to get access to the Hadoop resources that you need.
Then we need three things: keytab_path, keytab_principal and the kinit_keytab function:

```python
from livy_submit import kinit_keytab
# Define the principal in the keytab and the path to the keytab that we've uploaded
keytab_path = '/opt/continuum/project/livy-submit/rest-api/edill.keytab'
keytab_principal = 'user@DEMO.COM'
# Show that the kinit_keytab function works as expected
kinit_keytab(keytab_path, keytab_principal)
```

If you prefer to use a username / pass to kinit, then follow the instructions in our [enterprise docs](https://enterprise-docs.anaconda.com/en/latest/data-science-workflows/user-settings.html?highlight=secret#storing-secrets) to add your Kerberos username and password to Anaconda Enterprise.

I would suggest that you name the secret "KRB_CREDS" and put your username and password in one per line. 
Then you can use the following code snippet to read in your username and password. 
After doing so, you can use the `kinit_username` function from `livy-submit`.

```python
from livy_submit import kinit_username
secrets_path = '/var/run/secrets/user_credentials/KRB5_CREDS'
with open(secrets_path, 'r') as f:
    user, pw = [_.strip() for _ in f.readlines()]
print('user=%s, pw=%s' % (user, '*' * len(pw)))
# Show that the kinit_keytab function works as expected
kinit_username(user, pw)
```

## 3. Define REST endpoint function

In this example, we are going to publish one function that responds to GET requests. 
In this function we will first kinit to ensure that we always have a valid Kerberos ticket. 
Then we'll upload our file that we want to execute. 
Then we create a livy api object.
Next we will use that livy api object to submit the file and finally we return the Batch object that contains the livy info that we need to monitor it's status elsewhere.

```python
from tranquilizer import tranquilize

# Define the file that we want to execute on Yarn with livy submit
LOCAL_FILE = '/opt/continuum/project/livy-submit/02-flights-per-day.py'

@tranquilize(method='GET')
def submit():
    # Use kinit_keytab or kinit_username, depending on your preference
    kinit_keytab(keytab_path, keytab_principal)
    file_on_hdfs = upload(LOCAL_FILE, namenode_url=namenode_url)
    livy_api = LivyAPI(server_url=livy_url)
    batch = livy_api.submit(name='livy test', 
                            file=file_on_hdfs)
    return json.dumps(repr(batch))
```
One of the great things about the `tranquilizer` project is that the decorator syntax still let's you use the `submit` function in this notebook so that you can easily debug your function. 
Then, when you're ready to deploy it, just remove all of your debugging code and the endpoint works as expected!

```python
# For debugging, we'll want to run this submit function to make sure our code works
submit()
```

For more reading on tranquilizer, have a look at the [tranquilizer readme on github](https://github.com/AlbertDeFusco/tranquilizer). There is much more information there.

## 4. Publish with tranquilizer
When you're ready to publish this REST API, you'd want to add the following command to your anaconda-project:

```
  livy-submit-rest-api:
    unix: tranquilizer path/to/this/notebook.ipynb --name "livy-submit-rest-api"
    supports_http_options: true
```

Then you'll need to commit the project and [deploy](https://enterprise-docs.anaconda.com/en/latest/data-science-workflows/deployments/project.html#to-deploy-a-project) the "livy-submit-rest-api" command.

If you're not using Anaconda Enterprise, then deploying requires you to run `tranquilizer path/to/this/notebook.ipynb` and all of the other parts of deploying and securing your deployment are left up to you, the reader.

## 5. Testing the deployment
If you're using Anaconda Enterprise, then you will have chosen a fixed URL for your deployment, let's say `curl -X GET https://livy-submit.demo.anaconda.com/submit.
This will return a livy batch ID.
Use that with `livy log -f <batchId>` to watch the logs from your triggered Spark job.

TODO: Add instructions for using the AE5 token for auth'd deployments
TODO: Add full notebook or anaconda project for people to download