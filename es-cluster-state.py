# Check Cluster State
# ====
# This notebook check the state of ES cluster and if needed creates alarm.
# It is run once per hour from a cron job.

import sys
from alerts import alarms
from elasticsearch import Elasticsearch

# load ES_HOST, ES_USER, ES_PASS from environment
import os
env = {}
for var in ['ES_HOST', 'ES_USER', 'ES_PASS']:
    env[var] = os.environ.get(var, None)
    if not env[var]:
        print('environment variable {} not set!'.format(var))
        sys.exit(1)

es = Elasticsearch(
    hosts=[{'host': env['ES_HOST'], 'port': 9200, 'scheme': 'https'}],
    basic_auth=(env['ES_USER'], env['ES_PASS']),
    request_timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

report = es.health_report()
print(report)


if report['status'] == 'green':
    sys.exit(0)

ALARM = alarms('Analytics', 'Elasticsearch', 'status')

if report['status'] == 'red':
    ALARM.addAlarm(
        body='Alert on Elastic cluster state [ES in red]', tags=['red'])
if report['status'] == 'yellow':
    ALARM.addAlarm(
        body='Alert on Elastic cluster state [ES in yellow]', tags=['yellow'])
