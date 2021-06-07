# Checks number of failed queries (rejected/unprocessed queries and DB disconnections) (TEST)
# ====
# This notebook checks if there are failed queries:
# - Rejected queries: server is busy and doesn't respond to the query
# - DB disconnections: the query was processed by the Frontier server but the Oracle DB
#   terminated the connection
# - Unprocessed queries: Oracle DB returned data, but it wasn't sent to the querying job
#
# It is run every hour from a cron job.

import sys
import datetime
from alerts import alarms
from elasticsearch import Elasticsearch
# from elasticsearch.helpers import scan


import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)


# Period to check from now backwards
nhours = 1
# Limit of unsatisfied queries on a given server
ntotfail = 100
# Limit of unsatisfied queries for a given task
ntottask = 100

# Following 2 lines are for testing purposes only
# curtime = '20170126T120000.000Z'
# ct = datetime.datetime.strptime(curtime, "%Y%m%dT%H%M%S.%fZ")


# ### Get starting and current time for query interval
#
# We need :
# 1. Current UTC time (as set in timestamp on ES DB)
# 2. Previous date stamp (**nhours** ago) obtained from a time delta
#
# In order to subtract the time difference we need **ct** to be a datetime object


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
# Send a query to the ES-DB for documents containing information of failed queries

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

condition = 'rejected:true OR disconn:true OR procerror:true'

my_query = {
    "size": 0,
    "query": {
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
                "unserved": {
                    "filters": {
                        "filters": {
                            "rejected": {
                                "query_string": {
                                    "query": "rejected:true"
                                }
                            },
                            "disconnect": {
                                "query_string": {
                                    "query": "disconn:true"
                                }
                            },
                            "procerror": {
                                "query_string": {
                                    "query": "procerror:true"
                                }
                            }
                        }
                    },
                    "aggs": {
                        "taskid": {
                            "terms": {
                                "field": "taskid",
                                "size": 5,
                                "order": {
                                    "_count": "desc"
                                }
                            },
                            "aggs": {
                                "taskname": {
                                    "terms": {
                                        "field": "taskname",
                                        "size": 5,
                                        "order": {
                                            "_count": "desc"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

res = es.search(index=ind, body=my_query)
res = res['aggregations']['servers']['buckets']

taskinfo = {}

# Loop over Frontier servers

for r in res:

    tkid = r['unserved']['buckets']['rejected']['taskid']['buckets']
    for ti in tkid:
        tkname = ti['taskname']['buckets']
        for tn in tkname:
            if ti['key'] not in taskinfo:
                taskinfo[ti['key']] = [tn['key'], [int(tn['doc_count']), 0, 0]]
            else:
                count = int(taskinfo[ti['key']][1][0])
                taskinfo[ti['key']][1][0] = count + int(tn['doc_count'])

    tkid = r['unserved']['buckets']['disconnect']['taskid']['buckets']
    for ti in tkid:
        tkname = ti['taskname']['buckets']
        for tn in tkname:
            if ti['key'] not in taskinfo:
                taskinfo[ti['key']] = [tn['key'], [0, int(tn['doc_count']), 0]]
            else:
                count = int(taskinfo[ti['key']][1][1])
                taskinfo[ti['key']][1][1] = count + int(tn['doc_count'])

    tkid = r['unserved']['buckets']['procerror']['taskid']['buckets']
    for ti in tkid:
        tkname = ti['taskname']['buckets']
        for tn in tkname:
            if ti['key'] not in taskinfo:
                taskinfo[ti['key']] = [tn['key'], [0, 0, int(tn['doc_count'])]]
            else:
                count = int(taskinfo[ti['key']][1][2])
                taskinfo[ti['key']][1][2] = count + int(tn['doc_count'])

taskid = {}
for key in taskinfo:
    if sum(taskinfo[key][1]) > ntottask:
        taskid[key] = taskinfo[key]

print('problematic tasks:', taskid)

frontiersrvr = {}
frsrvs = []
for r in res:
    ub = r['unserved']['buckets']
    rej = ub['rejected']['doc_count']
#    if rej>0:
#        print(ub['rejected']['taskid'])
    dis = ub['disconnect']['doc_count']
#    if dis>0:
#        print(ub['rejected']['taskid'])
    pre = ub['procerror']['doc_count']
#    if pre>0:
#        print(ub['rejected']['taskid'])
    if rej + dis + pre < ntotfail:
        continue
    mes = ''
    if rej > 0:
        mes += str(rej) + " rejected "
    if dis > 0:
        mes += str(dis) + " disconnected "
    if pre > 0:
        mes += str(pre) + " unprocessed "
    frontiersrvr[r['key']] = mes + 'queries.'
    frsrvs.append(r['key'])

print('problematic servers:', frontiersrvr)


# ### Any non-zero value for any Frontier server triggers the alert
#
# The alert contains every Frontier server with failed queries and which kind of failures happened.


if len(frontiersrvr) > 0 or len(taskid) > 0:

    ALARM = alarms('Analytics', 'Frontier', 'Failed queries')
    ALARM.addAlarm(
        body='Failed Frontier queries',
        tags=frsrvs,
        source={'servers': frontiersrvr, 'tasks': taskid}
    )

    #   body += '\tthis mail is to let you know that in the past ' + \
    #        str(nhours) + ' hours \n'
    #    if len(frontiersrvr) > 0:
    #         body += '\tthe following servers present failed queries: \n'
    #         body += '\t(attached numbers correspond to rejected, disconnected and unprocessed queries) \n\n'
    #         for fkey in frontiersrvr:
    #             body += fkey
    #             body += ' : '
    #             body += frontiersrvr[fkey]
    #             body += '\n'
    #     body += '\n'
    #     if len(taskid) > 0:
    #         body += '\tthe following tasks present not completed requests: \n'
    #         body += '\n'
    #         for tkey in taskid:
    #             body += 'Task id ' + \
    #                 str(tkey) + ' with name ' + \
    #                 taskid[tkey][0] + ' has ' + \
    #                 str(taskid[tkey][1][0]) + ' rejected '
    #             body += str(taskid[tkey][1][1]) + ' disconnected and ' + \
    #                 str(taskid[tkey][1][2]) + ' unprocessed queries \n'
    #             body += 'http://bigpanda.cern.ch/tasknew/' + str(tkey) + '\n'
    #     body += '\nConsult the following link to get a table with the most relevant taskids (beware that\n'
    #     body += 'you will have to select the appropriate time period in the upper right corner)\n'
    #     body += 'https://atlas-kibana.mwt2.org:5601/s/frontier/goto/c72d263c3e2b86f394ab99211c99b613\n'
