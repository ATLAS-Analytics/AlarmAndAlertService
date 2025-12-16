# Checks if vP reported any servers going dead
# ====
# This code checks for any documents with live:false in
# vp_liveness index. It is run every 30 min from a cron job.

import sys
from datetime import datetime
from alerts import alarms
from elasticsearch import Elasticsearch


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

ALARM = alarms('Virtual Placement', 'XCache', 'dead server')

ct = datetime.now()
currentTime = int(round(datetime.now().timestamp() * 1000))
lastHours = 0.5
startTime = currentTime - int(lastHours * 3600000)
print('start time', startTime)
print('current time', datetime.now())


# vp_liveness

liveness_query = {
    "bool": {
        "must": [
            {"range": {"timestamp": {"gte": startTime, "lte": currentTime}}},
            {"term": {"live": False}}
        ]
    }
}

res = es.search(index='vp_liveness', query=liveness_query, size=100)
if res['hits']['total']['value'] == 0:
    print('All is fine.')
else:
    hits = res['hits']['hits']
    for hit in hits:
        site = hit['_source']['site']
        id = hit['_source']['id']
        address = hit['_source']['address']
        timestamp = datetime.fromtimestamp(hit['_source']['timestamp']/1000)
        source = {
            "site": site,
            "id": id,
            "address": address,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }
        print(source)
        ALARM.addAlarm(
            body='dead server.',
            tags=[site],
            source=source
        )
