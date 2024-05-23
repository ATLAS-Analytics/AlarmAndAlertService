# Check Cluster State
# ====
# This notebook check the state of ES cluster and if needed creates alarm.
# It is run once per hour from a cron job.

import sys
from alerts import alarms
from elasticsearch import Elasticsearch

import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'port':9200, 'scheme':'https'}],
    basic_auth=(config['ES_USER'], config['ES_PASS']),
    request_timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

report=es.health_report()
print(report)


if report['status'] == 'green':
    sys.exit(0)

ALARM = alarms('Analytics', 'Elasticsearch', 'status')

if report['status'] == 'red':
    ALARM.addAlarm(body='Alert on Elastic cluster state [ES in red]', tags=['red'])
if report['status'] == 'yellow':
    ALARM.addAlarm(body='Alert on Elastic cluster state [ES in yellow]', tags=['yellow'])
