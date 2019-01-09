#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

from random import random
from operator import add


#def run(spark, partitions):
#    n = 100000 * partitions
#
#    def f(_):
#        x = random() * 2 - 1
#        y = random() * 2 - 1
#        return 1 if x ** 2 + y ** 2 <= 1 else 0
#
#    count = (
#        spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
#    )
#    
#    return (4.0 * count / n)
#

from pi_runner import run

if __name__ == "__main__":
    """Usage: pi [partitions]"""
    import os
    import socket
    from pprint import pprint, pformat
    import subprocess
#     print(subprocess.check_output('hadoop ')
#     output_path = open('./data.txt', 'w')
    import sys
    output_path = sys.stdout
    print('\n\ncurrent hostname=%s' % socket.getfqdn(), file=output_path)
    print('\n\ncurrent working directory=%s\n\n' % os.getcwd(), file=output_path)
    print('\n\ncontents of CWD=%s\n\n' % (list(os.listdir('.')),), file=output_path)
    spark = SparkSession.builder.getOrCreate()
    
    print('\n\nSpark config: %s' % pformat(spark.sparkContext._conf.getAll()))
    
    print("Created spark session")
    print('\n\ncurrent working directory=%s\n\n' % os.getcwd(), file=output_path)
    print('\n\ncontents of CWD=%s\n\n' % (list(os.listdir('.')),), file=output_path)
    print('env', file=output_path)
    print(pformat(dict(os.environ)))

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    
    pi = run(spark, partitions)
    
    print("Pi is roughly %f" % pi, file=output_path)

    subprocess.check_call('hdfs dfs -put %s' % output_path, shell=True)
    
    spark.stop()
