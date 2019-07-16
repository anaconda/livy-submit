import requests
from requests_kerberos import HTTPKerberosAuth
from time import sleep
import json
import cloudpickle
import base64
import pickle

def _unpickle(result):
    step1 = base64.b64decode(result)
    step2 = base64.b64decode(step1)
    final = cloudpickle.loads(step2)
    return final

class LivyException(RuntimeError):
    pass

class AuthenticationError(LivyException):
    pass

class LivySessionNotFound(LivyException):
    pass


class pyspark(object):
    def __init__(self, session):
        self.session = session

    def __call__(self, func):
        def _(*args, **kwargs):
            def _f(spark):
                return func(*args, **kwargs)

            result, _ = self.session.submit(_f, run=False)
            return result

        return _


class LivySession(object):
    def __init__(self, host, name=None, session_id=None, wait=False):
        self.host = host
        self.auth = HTTPKerberosAuth()
        self.name = name
        self.headers = {'Content-Type': 'application/json'}

        if session_id:
            self.session_id = session_id
            print(f'Connecting to session {self.session_id}')
        else:
            data = {
                'conf': {
                    "spark.pyspark.python": "/opt/anaconda3/bin/python",
                },
                "executorMemory": "1000M", "executorCores": 4, "numExecutors": 1
            }

            if self.name:
                data['name'] = self.name

            r = requests.post(self.host + '/sessions',
                              data=json.dumps(data),
                              headers=self.headers,
                              auth=self.auth
                             )
            if r.status_code == 401:
                raise AuthenticationError('Authentication required.')
            else:
                self.session_id = r.json()['id']
                print(f'Starting session {self.session_id}')

        self.session_url = self.host + f'/sessions/{self.session_id}'
        if wait:
            r = self._wait(self.session_url, 'idle')
            print(f'Session {self.session_id} ready.')


    def _wait(self, url, expected_state):
        print(f'Waiting for {url}')
        while True:
            r = requests.get(url,
                             headers=self.headers,
                             auth=self.auth)
            if r:
                state = r.json()['state']
            elif r.status_code == 404:
                raise LivySessionNotFound(f'{url} was not found.')
            else:
                print(r)
                print(r.text)
                raise LivyException(f'problem waiting for {url}')

            if not r:
                raise LivyException(f'{url}: {r.status_code}')
            if state==expected_state:
                break
            sleep(5)
        return r


    def state(self):
        r = requests.get(self.session_url, headers=self.headers, auth=self.auth)
        return r.json()

    def submit(self, job, kind='pyspark', run=True):
        _ = self._wait(self.session_url, 'idle')

        pickled_job = cloudpickle.dumps(job)
        base64_pickled_job = base64.b64encode(pickled_job).decode('utf-8')
        data =  {
            'job': base64_pickled_job,
            'jobType': kind
        }

        if run:
            command = 'run-job'
        else:
            command = 'submit-job'

        r = requests.post(self.session_url + f'/{command}',
                          json=data,
                          headers=self.headers,
                          auth=self.auth)

        if r:
            jobid = r.json()['id']
        else:
            print(r)
            print(r.url)
            print(r.content)
            print(r.reason)
            print(r.request)
            print(r.request.body)
            print(r.headers)
            raise LivyException('problem with job')

        job_url = f'{self.session_url}/jobs/{jobid}'
        r = self._wait(job_url, expected_state='SUCCEEDED')

        j = r.json()
        result = _unpickle(j['result'])
        error = j['error']

        return result, error

    def execute(self, code, kind='pyspark'):
        _ = self._wait(self.session_url, 'idle')

        data = {
            'code': code,
            'kind': kind
        }
        r = requests.post(self.session_url + '/statements',
                          data=json.dumps(data),
                          headers=self.headers,
                          auth=self.auth)
        statement_url = self.host + r.headers['location']
        r = self._wait(statement_url, 'available')
        return r.json()


    def delete(self):
        try:
            _ = self._wait(self.session_url, 'idle')
        except LivySessionNotFound:
            return 404


        r = requests.delete(self.session_url,
                            headers=self.headers,
                            auth=self.auth)
        return r.status_code
