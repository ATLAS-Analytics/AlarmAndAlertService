# Checks number of simultaneous threads (TEST)
# ====
# Checks whether the number of simultaneous threads reaches a number beyond **threadlimit**. It sends mails to all the people substribed to that alert. It is run every half an hour from a cron job (not yet).

import datetime

from subscribers import subscribers
import alerts

from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)


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
ind = 'frontier-new-%d-%02d' % (ct.year, ct.month)
print(ind)
curtime = ct.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

td = datetime.timedelta(hours=nhours)
st = ct - td
starttime = st.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

print('start time', starttime)
print('current time', curtime)


# ### Establish connection to ES-DB and submit query
#
# Send a query to the ES-DB to get the highest number of simultaneous threads beyond the limit imposed by **threadlimit** on each Frontier server for the given time interval


es = Elasticsearch(hosts=[{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)

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

res = es.search(index=ind, body=my_query, request_timeout=600)

frontiersrvr = {}
res = res['aggregations']['servers']['buckets']
for r in res:
    print(r)
    if r['maxthreads']['value'] > threadlimit:
        frontiersrvr[r['key']] = r['maxthreads']['value']

print('problematic servers:', frontiersrvr)


# ### Submit alert if there are any servers showing a high number of simultaneous threads (>**threadlimit**)
#
# The number associated to each Frontier server is the highest number recorded during the given time interval

if len(frontiersrvr) > 0:
    S = subscribers()
    A = alerts.alerts()

    test_name = 'Too many concurrent threads'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you know that the number of simultaneous threads went beyond '
        body += str(threadlimit) + ' on some servers \n\n'
        for fkey in frontiersrvr:
            body += fkey
            body += ' : '
            body += str(frontiersrvr[fkey])
            body += '\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please use the following link:\n' + user.link
        A.sendGunMail(test_name, user.email, body)
##        A.addAlert(test_name, user.name, str(res_page))
