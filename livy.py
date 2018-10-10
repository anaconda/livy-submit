import time
import os
import sys
import requests
import kerberos
import requests_kerberos
import json, pprint, textwrap

user = 'edill'
local_file_path = 'pi.py'
local_filename = os.path.basename(local_file_path)

hdfs_destination = f'/user/{user}/{local_filename}'
tmp_path = f'/tmp/{local_filename}'

# Upload run.py to hdfs
from hdfs.ext.kerberos import KerberosClient
nn_fqdn = 'cdh-nn1.dev.anaconda.com'
client = KerberosClient(f'http://{nn_fqdn}:50070')
print(f"Contents of {user}'s home directory")
print(client.list(f'/user/{user}'))

print("Uploading {local_file_path} to {hdfs_destination}")
client.upload(f'{hdfs_destination}', local_file_path, overwrite=True)
client.download(f'{hdfs_destination}', tmp_path, overwrite=True)

with open(local_filename, 'r') as f:
    for line in f.readlines():
        print(line.strip('\n'))

#host = '10.200.30.212:8998'
#host = 'http://cdh-livy1.salab.anaconda.com:8998'
host = 'http://cdh-edge1.dev.anaconda.com:8998'
#host = 'http://cdh-edge-bzaitlen.dev.anaconda.com:8765'
auth = requests_kerberos.HTTPKerberosAuth(
    mutual_authentication=requests_kerberos.REQUIRED,
    force_preemptive=True)

headers = {'Content-Type': 'application/json'}

#r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers, auth=auth)

data = {"kind":"pyspark"}
data = {
    'file': f'hdfs://{hdfs_destination}',
    'name': 'livy batches endpoint test',
    'args': sys.argv[1:]
}

endpoint = 'sessions'
endpoint = 'batches'

data = json.dumps(data)
#r = requests.get(host + f"/{endpoint}", headers=headers, auth=auth)
r = requests.post(host + f'/{endpoint}', headers=headers, auth=auth, data=data)
print(r.text)

def get_id(response_json):
    return response_json['id']

r.raise_for_status()
response_json = r.json()
submission_id = get_id(r.json())

last_stop = 0
for i in range(100):
    r = requests.get(host + f"/batches/{submission_id}/log", headers=headers, auth=auth)
    r.raise_for_status()
    logs_json = r.json()
    start = logs_json['from']
    stop = logs_json['total']
    logs = logs_json['log']
    #print('logs array has %s elements' % len(logs))
    for line in logs[(1 + last_stop):]:
        if line == '\nstderr: ':
            break
        print(line)
    last_stop = stop
    time.sleep(1)
        
