# Checks if Panda jobs and taks data are indexed
# ====
# This notebook checks number of indexed documents in jobs and panda tables
# and creates alarm if any of them is 0. It is run every 6h from a cron job.

import sys
import json
from datetime import datetime
from alerts import alarms
from elasticsearch import Elasticsearch, exceptions as es_exceptions


config_path = '/config/config.json'
# config_path = 'kube/secrets/config.json'

with open(config_path) as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

ALARM = alarms('Analytics', 'WFMS', 'indexing')

ct = datetime.now()
currentTime = int(round(datetime.now().timestamp() * 1000))
lastHours = 7
startTime = currentTime - lastHours * 3600000
print('start time', startTime)
print('current time', datetime.now())


# JOBS

jobs_query = {
    "query": {
        "range": {"modificationtime": {"gte": startTime, "lte": currentTime}}
    }
}

res = es.count(index='jobs', body=jobs_query)
print(res)
if res['count'] == 0:
    ALARM.addAlarm(body='Issue in indexing jobs.', tags=['jobs'])


# TASKS

tasks_query = {
    "query": {
        "range": {"modificationtime": {"gte": startTime, "lte": currentTime}}
    }
}

res = es.count(index='tasks', body=tasks_query)
print(res)
if res['count'] == 0:
    ALARM.addAlarm(body='Issue in indexing tasks.', tags=['tasks'])


# TASK PARAMETERS

task_params_query = {
    "query": {
        "range": {"creationdate": {"gte": startTime, "lte": currentTime}}
    }
}

res = es.count(index='task_parameters', body=tasks_query)
print(res)
if res['count'] == 0:
    ALARM.addAlarm(body='Issue in indexing task parameters.',
                   tags=['task parameters'])
