# Check Cluster State
# ====
# This notebook check the state of ES cluster and if needed creates alarm.
# It is run once per hour from a cron job.

import sys
from alerts import alarms
import requests

import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)

ES_CONN = 'https://' + config['ES_USER'] + ':' + config['ES_PASS'] + \
    '@' + config['ES_HOST'] + ':9200/_cluster/health'
r = requests.get(ES_CONN)
res = r.json()
print(res)


if res['status'] == 'green':
    sys.exit(0)

ALARM = alarms('Analytics', 'Elasticsearch', 'status')

if res['status'] == 'red':
    ALARM.addAlarm(body='Alert on Elastic cluster state [ES in red]', tags=['red'])
if res['status'] == 'yellow' and res['unassigned_shards'] > 10:
    ALARM.addAlarm(body='Alert on Elastic cluster state [ES in yellow]', tags=['yellow'])
