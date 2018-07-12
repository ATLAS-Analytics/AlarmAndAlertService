# Checks number of failed queries (rejected/unprocessed queries and DB disconnections) (TEST)
# ====
# This notebook checks if there are failed queries:
# - Rejected queries: server is busy and doesn't respond to the query
# - DB disconnections: the query was processed by the Frontier server but the Oracle DB terminated the connection
# - Unprocessed queries: Oracle DB returned data, but it wasn't sent to the querying job
#
# It sends mails to all the people substribed to that alert. It is run every half an hour from a cron job (not yet).

import datetime

from subscribers import subscribers
import alerts

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import scan

# Period to check from now backwards
nhours = 1
# Limit of unsatisfied queries on a given server
ntotfail = 100
# Limit of unsatisfied queries for a given task
ntottask = 100

# Following 2 lines are for testing purposes only
#curtime = '20170126T120000.000Z'
#ct = datetime.datetime.strptime(curtime, "%Y%m%dT%H%M%S.%fZ")


# ### Get starting and current time for query interval
#
# We need :
# 1. Current UTC time (as set in timestamp on ES DB)
# 2. Previous date stamp (**nhours** ago) obtained from a time delta
#
# In order to subtract the time difference we need **ct** to be a datetime object


ct = datetime.datetime.utcnow()
ind = 'frontier-new-%d-%02d' % (ct.year, ct.month)
print(ind)
curtime = ct.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

td = datetime.timedelta(hours=nhours)
st = ct - td
starttime = st.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

#####################
#ind = 'frontier-new-*'
#td = datetime.timedelta(days=13)
#nct = ct - td
#td = datetime.timedelta(days=3)
#nst = nct - td
#curtime = nct.strftime('%Y%m%dT%H%M%S.%f')[:-3]+'Z'
#starttime = nst.strftime('%Y%m%dT%H%M%S.%f')[:-3]+'Z'
#####################

print('start time', starttime)
print('current time', curtime)


# ### Establish connection to ES-DB and submit query
#
# Send a query to the ES-DB for documents containing information of failed queries

es = Elasticsearch(hosts=[{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)

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

res = es.search(index=ind, body=my_query, request_timeout=600)
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
        mes += str(rej) + " rejected\t"
    if dis > 0:
        mes += str(dis) + " disconnected\t"
    if pre > 0:
        mes += str(pre) + " unprocessed "
    frontiersrvr[r['key']] = mes + 'queries.'

print('problematic servers:', frontiersrvr)


# ### Any non-zero value for any Frontier server triggers the alert
#
# The alert contains every Frontier server with failed queries and which kind of failures happened.

# In[198]:


if len(frontiersrvr) > 0 or len(taskid) > 0:
    S = subscribers()
    A = alerts.alerts()

    test_name = 'Failed queries'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you know that in the past ' + str(nhours) + ' hours \n'
        if len(frontiersrvr) > 0:
            body += '\tthe following servers present failed queries: \n'
            body += '\t(attached numbers correspond to rejected, disconnected and unprocessed queries) \n\n'
            for fkey in frontiersrvr:
                body += fkey
                body += ' : '
                body += frontiersrvr[fkey]
                body += '\n'
        body += '\n'
        if len(taskid) > 0:
            body += '\tthe following tasks present not completed requests: \n'
            body += '\n'
            for tkey in taskid:
                body += 'Task id ' + str(tkey) + ' with name ' + taskid[tkey][0] + ' has ' + str(taskid[tkey][1][0]) + ' rejected '
                body += str(taskid[tkey][1][1]) + ' disconnected and ' + str(taskid[tkey][1][2]) + ' unprocessed queries \n'
                body += 'http://bigpanda.cern.ch/task/' + str(tkey) + '\n'
        body += '\nConsult the following link to get a table with the 3 most relevant taskids (beware that\n'
        body += 'you will have to select the appropriate time period in the upper right corner)\n'
        body += 'http://atlas-kibana-dev.mwt2.org/goto/fb8cf197b67498d2aee54df04bd79ce1\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please use the following link:\n' + user.link

        A.sendGunMail(test_name, user.email, body)
#        A.addAlert(test_name, user.name, str(res_page))
#    A.sendMail(test_name, "julio.lozano.bahilo@cern.ch", body)

#### BULSHIT ####
# # Defining the time period
# endtime_start_unix = 1520833680
# endtime_final_unix = 1520833800
# # Format for the elastic search
# endtime_start = datetime.datetime.fromtimestamp(int(endtime_start_unix)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
# endtime_final = datetime.datetime.fromtimestamp(int(endtime_final_unix)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

# print(endtime_start)
# print(endtime_final)

# myquery = {
#     "size": 10,
#     "query": {
#         "query_string": {
#             "query": "taskid:13251481",
#             "analyze_wildcard": True,
#             "lowercase_expanded_terms": False,
#         }
#     },
#     # "stored_fields": ["superstatus"]
# }

# res = es.search(index='frontier-new-*', body=myquery, request_timeout=600)
# print(len(res['hits']['hits']))
# for ientry in range(len(res['hits']['hits'])):
#     print(res['hits']['hits'][ientry]['_source']['sqlquery'])
