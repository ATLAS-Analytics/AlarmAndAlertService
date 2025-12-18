# Checks number of simultaneous threads (TEST)
# ====
# Checks whether the number of simultaneous threads reaches a number beyond **threadlimit**.
# It is run every half an hour from a cron job (not yet).

import sys
import datetime
from alerts import alarms
from elasticsearch import Elasticsearch

import os
from dotenv import load_dotenv

load_dotenv()
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

# ### Variables for script
#
# 1. Minimum number of simultaneous threads beyond which we submit the alert
# 2. Number of hours for query interval

# Thread limit to trigger an alarm
threadlimit = 400
# Period to check from now backwards
nhours = 1


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
#
# Send a query to the ES-DB to get the highest number of simultaneous threads beyond the limit
# imposed by **threadlimit** on each Frontier server for the given time interval

my_query = {
    "size": 0,
    "query": {
        #        "range":{"modificationtime":{"gte": starttime,"lte": curtime}}
        "range": {
            "@timestamp": {
                "gte": starttime,
                "lte": curtime,
                "format": "basic_date_time"
            }
        }
    },
    "aggs": {
        "servers": {
            "terms": {
                "size": 20,
                "field": "frontierserver"
            },
            "aggs": {
                "maxthreads": {
                    "max": {"field": "initthreads"}
                }
            }
        }
    }
}

res = es.search(index=ind, body=my_query)

frontiersrvr = {}
res = res['aggregations']['servers']['buckets']
for r in res:
    print(r)
    if r['maxthreads']['value'] > threadlimit:
        frontiersrvr[r['key']] = r['maxthreads']['value']

print('problematic servers:', frontiersrvr)


# ### Submit alert if there are any servers showing a high number of simultaneous threads
#
# The number associated to each Frontier server is the highest number recorded during
# the given interval

if len(frontiersrvr) > 0:
    ALARM = alarms('Analytics', 'Frontier', 'Too many threads')
    ALARM.addAlarm(
        body='Failed Frontier queries',
        tags=frontiersrvr,
        source={'servers': frontiersrvr}
    )
    # test_name = 'Too many concurrent threads'
    # body += '\tthis mail is to let you know that the number of simultaneous threads went beyond '
    # body += str(threadlimit) + ' on some servers \n\n'
    # for fkey in frontiersrvr:
    #     body += fkey
    #     body += ' : '
    #     body += str(frontiersrvr[fkey])
    #     body += '\n'
    # body += '\nBest regards,\nATLAS AAS'
    # body += '\n\n To change your alerts preferences please use the following link:\n' + user.link
    # A.sendGunMail(test_name, user.email, body)
