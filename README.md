sqljob
======

Run long-lasting SQL queries in the background.

## Installation

## Basic usage

```python
from sqljob import sqljob, Connector

# Pack a connection information
connector = Connector(sqlite3, "test.db")
job = sqljob("DROP TABLE IF EXISTS test", connector)

```

```python
import sqlite3
import time
from sqljob import sqljob, Connector

connector = Connector(sqlite3, "test.db")

job = sqljob("DROP TABLE IF EXISTS test", connector)
job.wait()
job = sqljob("CREATE TABLE test (x int, y text, z float)", connector)
job.wait()
job = sqljob("INSERT INTO test VALUES (1, 'hello', 3.14)", connector, postcommit=True).wait()
job = sqljob("INSERT INTO test VALUES (2, 'world', 2.718)", connector, postcommit=True).wait()

job = sqljob("SELECT * FROM test", connector)
# We have a control while the job is running
while job.running():
  print(".", end="")
  time.sleep(0.001)
print()
job.result_df
#    x      y      z
# 0  1  hello  3.140
# 1  2  world  2.718
```

## Use SQLAlchemy string

```python
connector = "sqlite:///test.db"
job = sqljob("SELECT * FROM test", connector).wait()
job.result_df
#    x      y      z
# 0  1  hello  3.140
# 1  2  world  2.718
```

## Activate logging

```python
from logging import basicConfig
basicConfig(level=20, format="%(level)s:%(asctime)s:%(message)s")
# job = sqljob("SELECT * FROM test", connector).wait()
# INFO:2021-06-02 10:51:48,687:Start SQL worker (id=7)
# INFO:2021-06-02 10:51:48,688:Start SQL job
# INFO:2021-06-02 10:51:48,691:Start running query
# SELECT * FROM test
# INFO:2021-06-02 10:51:48,692:Finish running query (Elapsed: 0:00:00.000363)
# INFO:2021-06-02 10:51:48,693:-1 rows has been affected
# INFO:2021-06-02 10:51:48,694:Fetch and write result table if any
# INFO:2021-06-02 10:51:48,699:Finish writing to CSV file '.../sqljob/sqljob-results/job_7_210602_105148.csv'
# INFO:2021-06-02 10:51:48,703:Data frame is written to '.../sqljob/sqljob-results/job_7_210602_105148.pkl'
# INFO:2021-06-02 10:51:48,704:End SQL job
```

# Query results

Results are saved as CSV and Pickle files in `./sqljob-results/` in the default setting.

```python
import os
os.listdir("sqljob-results")
# ['job_7_210602_105148.csv',
#  'job_6_210602_105147.pkl',
#  'job_5_210602_105147.pkl',
#  'job_5_210602_105147.csv',
#  'job_6_210602_105147.csv',
#  'job_7_210602_105148.pkl']
```
