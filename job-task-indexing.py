# Checks if Panda jobs and taks data are indexed
# ====
# This notebook checks number of indexed documents in jobs and panda tables and alerts if any of them is 0. It sends mails to all the people substribed to that alert. It is run every 6h from a cron job.

from datetime import datetime

from subscribers import subscribers
import alerts
from alarms import alarms

from elasticsearch import Elasticsearch, exceptions as es_exceptions


import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)


A = alerts.alerts()
ALARM = alarms('Analytics', 'WFMS', 'indexing')

ct = datetime.now()
currentTime = int(round(datetime.now().timestamp() * 1000))
lastHours = 7
startTime = currentTime - lastHours * 3600000
print('start time', startTime)
print('current time', datetime.now())


jobs_query = {
    "query": {
        "range": {"modificationtime": {"gte": startTime, "lte": currentTime}}
    }
}

res = es.count(index='jobs', body=jobs_query)
print(res)


if res['count'] == 0:
    S = subscribers()
    ALARM.addAlarm(body='Issue in indexing jobs.', tags=['jobs'])

    test_name = 'Alert on Elastic indexing rate [Panda Jobs]'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you that there is an issue in indexing Panda Jobs data in UC Elasticsearch.\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please you the following link:\n' + user.link
        A.sendGunMail(test_name, user.email, body)
        # A.addAlert(test_name, user.name, str(res))

tasks_query = {
    "query": {
        "range": {"modificationtime": {"gte": startTime, "lte": currentTime}}
    }
}

res = es.count(index='tasks', body=tasks_query)
print(res)


if res['count'] == 0:
    S = subscribers()
    A = alerts.alerts()
    ALARM.addAlarm(body='Issue in indexing tasks.', tags=['tasks'])

    test_name = 'Alert on Elastic indexing rate [Panda Tasks]'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you that there is an issue in indexing Jedi tasks data in UC Elasticsearch.\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please you the following link:\n' + user.link
        A.sendGunMail(test_name, user.email, body)
        # A.addAlert(test_name, user.name, str(res))

task_params_query = {
    "query": {
        "range": {"creationdate": {"gte": startTime, "lte": currentTime}}
    }
}

res = es.count(index='task_parameters', body=tasks_query)
print(res)


if res['count'] == 0:
    S = subscribers()
    A = alerts.alerts()
    ALARM.addAlarm(body='Issue in indexing task parameters.',
                   tags=['task parameters'])

    test_name = 'Alert on Elastic indexing rate [Panda Task Parameters]'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you that there is an issue in indexing Jedi task parameters data in UC Elasticsearch.\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please you the following link:\n' + user.link
        A.sendGunMail(test_name, user.email, body)
        # A.addAlert(test_name, user.name, str(res))
