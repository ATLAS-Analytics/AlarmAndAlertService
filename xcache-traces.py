# continuously looks up Rucio traces from UC ES,
# filters bad transfers through xcaches
# retries and classifies them
# ====

import sys
import datetime
from alerts import alarms
from elasticsearch import Elasticsearch

import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'port':9200, 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    request_timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

# Period to check from now backwards
nhours = 3

ct = datetime.datetime.utcnow()
ind = 'rucio-traces'
print(ind)
curtime = ct.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

td = datetime.timedelta(hours=nhours)
st = ct - td
starttime = st.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

print('start time', starttime)
print('current time', curtime)

my_query = {
    "size": 10000,
    "query": {
        "bool": {
            "must": [
                {
                    "wildcard": {"url": {"value": "root*//root*"}}
                },
                {
                    "term": {"clientState": "FAILED_REMOTE_OPEN"}
                },
                {
                    "range": {
                        "@timestamp": {
                            "gte": starttime,
                            "lte": curtime,
                            "format": "basic_date_time"
                        }
                    }
                }
            ]
        }
    }
}

res = es.search(index=ind, body=my_query)
results = res['hits']['total']['value']
print('total results:', results)

# tkid = []
# user = []
# tkids = {}
# users = {}
# for i in range(results):
#     tkid.append(res['hits']['hits'][i]['_source']['taskid'])
#     user.append(res['hits']['hits'][i]['_source']['dn'])

# for i in range(results):
#     if len(tkid) > 0:
#         count = tkid.count(tkid[i])
#         value = tkid[i]
#         for j in range(count):
#             tkid.remove(value)
#         tkids[value] = count

# for i in range(results):
#     if len(user) > 0:
#         count = user.count(user[i])
#         value = user[i]
#         for j in range(count):
#             user.remove(value)
#         users[value] = count

# if len(tkids) > 0:
#     ALARM = alarms('Analytics', 'Frontier', 'Bad SQL queries')
#     ALARM.addAlarm(
#         body='Bad SQL queries',
#         source={'users': list(users), 'tkids': list(tkids)}
#     )
