# Checks number of simultaneous threads (TEST)
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
    basic_auth=(config['ES_USER'], config['ES_PASS']),
    request_timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

# ### Variables for script
#
# 1. Minimum number of simultaneous threads beyond which we submit the alert
# 2. Number of hours for query interval

# Period to check from now backwards
nhours = 3
# Limit of incorrect SQL query constructions
nbadsql = 100

# ### Get starting and current time for query interval
#
# We need :
# 1. Current UTC time (as set in timestamp on ES DB)
# 2. Previous date stamp (**nhours** ago) obtained from a time delta
#
# In order to subtract the time difference we need **ct** to be a datetime object

# Get current UTC time (as set in timestamp on ES DB)
# In order to subtract the time difference we need ct to be a datetime object

ct = datetime.datetime.utcnow()
ind = 'frontier_sql'
print(ind)
curtime = ct.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

td = datetime.timedelta(hours=nhours)
st = ct - td
starttime = st.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

print('start time', starttime)
print('current time', curtime)


# ### Establish connection to ES-DB and submit query

my_query = {
    "size": 10000,
    "query": {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "query": "sqlquery:OPT_PARAM\\('OPTIMIZER_ADAPTIVE_FEATURES','FALSE'\\)",
                        "analyze_wildcard": True
                    }
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

tkid = []
user = []
tkids = {}
users = {}
for i in range(results):
    tkid.append(res['hits']['hits'][i]['_source']['taskid'])
    user.append(res['hits']['hits'][i]['_source']['dn'])

for i in range(results):
    if len(tkid) > 0:
        count = tkid.count(tkid[i])
        value = tkid[i]
        for j in range(count):
            tkid.remove(value)
        tkids[value] = count

for i in range(results):
    if len(user) > 0:
        count = user.count(user[i])
        value = user[i]
        for j in range(count):
            user.remove(value)
        users[value] = count

if len(tkids) > 0:
    ALARM = alarms('Analytics', 'Frontier', 'Bad SQL queries')
    ALARM.addAlarm(
        body='Bad SQL queries',
        source={'users': list(users), 'tkids': list(tkids)}
    )
