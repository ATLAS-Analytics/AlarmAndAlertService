# <h1>This notebook retrieves from ES the info from jobs_archive about 10 top users, and sends alarm if usage is above certain thresholds</h1>

import alerts
from subscribers import subscribers
import subprocess
import json
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from pandas.io.json import json_normalize
from pandas import DataFrame
import pandas as pd
from datetime import datetime, timedelta
import datetime

import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)

# ## Establish Elasticsearch connection
es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)
# ## Alerts and Alarms


S = subscribers()
A = alerts.alerts()

ind = 'jobs'

# <h2>First Alarm</h2>
# <h3>get top 10 users/24 hours for walltime*core, and filter out sum walltime > 15 years</h3>
# <h3>convert walltime in number of cores used per day, by assuming all jobs are single core</h3>

s = {
    "size": 0,
    'query': {
        'bool': {
            'must': [
                {"term": {"prodsourcelabel": "user"}},
                {'range': {
                    'modificationtime': {
                        "gte": "now-1d",
                        "lt":  "now"}
                }
                },
                {'bool': {
                    'must_not': [
                        {"term": {"produsername": "gangarbt"}},
                        {"term": {"processingtype": "pmerge"}},
                        # only users without workinggroup priviledges
                        {'exists': {"field": "workinggroup"}}
                    ]
                }
                }
            ],
        }
    },
    "aggs": {
        "users": {
            "terms": {
                "field": "produsername",
                "order": {"walltime_core_sum": "desc"},
                "size": 10
            },
            "aggs": {
                "walltime_core_sum": {
                    "sum": {
                        "script": {   # use scripted field to calculate corecount
                            "inline": "def core=doc['actualcorecount'].value; if (core!=null) {return doc['wall_time'].value * core} else {return doc['wall_time'].value}"
                        }
                    }

                },
            }
        }
    }
}

res = es.search(index=ind, body=s)
# print(res)

agg = res['aggregations']['users']['buckets']
jsondata = json.dumps(agg)

process = subprocess.Popen(["curl", "-D-", "-H", "Content-Type:application/json", "-X", "POST", "--data", jsondata,
                            "http://test-jgarcian.web.cern.ch/test-jgarcian/cgi-bin/usersJIRA.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()
# print(agg)

# create df
df_w = json_normalize(agg)
df_w['walltime_core_sum.value'] = df_w['walltime_core_sum.value'].apply(
    lambda x: timedelta(seconds=int(x)).days / 365.2)
df_w['ncores'] = df_w['walltime_core_sum.value'].apply(
    lambda x: x * 365.)  # transform walltime[year] in walltime[day]

LIMIT_WALLTIME = 15  # 5 for testing
df_w = df_w[df_w["walltime_core_sum.value"] > LIMIT_WALLTIME]

df_w.columns = ['jobs', 'user', 'walltime used [years]', 'number of cores']
print(df_w.to_string())


if df_w.shape[0] > 0:
    test_name = 'Top Analysis users [Large wall time]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name + ',\n\n'
        body += 'the following users used substantial wall time (more than 15 years/last 24 hours, corresponding to 5475 cores/day):\n\n'
        body += df_w.to_string() + '\n'
        body += '\n To get more information about this alert message and its interpretation, please visit:\n'
        body += 'https://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis-User'
        body += '\nhttps://its.cern.ch/jira/browse/ADCDPA-1'
        body += '\n To change your alerts preferences please use the following link:\n' + u.link
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        #A.sendMail(test_name, u.email, body)
        # print(body)
    #A.addAlert(test_name, u.name, str(df_w.shape[0])+' users with huge walltime.')
else:
    print('No Alarm')


# <h2>Second Alarm</h2>
# <h3>get top 10 users/24 hours for inputfilebytes, and filter out sum input size > 500 TB</h3>


s = {
    "size": 0,  # get one job entry only for debugging purposes
    'query': {
        'bool': {
            'must': [
                {"term": {"prodsourcelabel": "user"}},
                {'range': {
                    'modificationtime': {
                        "gte": "now-1d",
                        "lt":  "now"}
                }
                },
                {'bool': {
                    'must_not': [
                        {"term": {"produsername": "gangarbt"}},
                        {"term": {"processingtype": "pmerge"}},
                        {"term": {"jobstatus": "closed"}},
                        {"term": {"jobstatus": "cancelled"}},
                        {'exists': {"field": "workinggroup"}}]
                }
                }
            ],
        }
    },
    "aggs": {
        "users": {
            "terms": {
                "field": "produsername",
                "order": {"inputsize_sum": "desc"},
                "size": 10
            },
            "aggs": {
                "inputsize_sum": {
                    "sum": {"field": "inputfilebytes"}
                },
            }
        }
    }
}

res = es.search(index=ind, body=s)
# print(res)


agg = res['aggregations']['users']['buckets']
# print(agg)

jsondata = json.dumps(agg)

process = subprocess.Popen(["curl", "-D-", "-H", "Content-Type:application/json", "-X", "POST", "--data", jsondata,
                            "http://test-jgarcian.web.cern.ch/test-jgarcian/cgi-bin/usersJIRA.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()
print(stdout)

# create df
df_i = json_normalize(agg)
df_i['inputsize_sum.value'] = df_i['inputsize_sum.value'].apply(
    lambda x: x * 0.00000000000089)

LIMIT_INPUTSIZE = 500  # 5 for testing
df_i = df_i[df_i["inputsize_sum.value"] > LIMIT_INPUTSIZE]

df_i.columns = ['jobs', 'input size [TB]', 'user']
print(df_i.to_string())


if df_i.shape[0] > 0:
    test_name = 'Top Analysis users [Large input data size]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name + ',\n\n'
        body += 'the following users processed rather substantial input data (>500 TB/last 24 hours):\n\n'
        body += df_i.to_string() + '\n'
        body += '\n To get more information about this alert message and its interpretation, please visit:\n'
        body += 'https://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis-User'
        body += '\nhttps://its.cern.ch/jira/browse/ADCDPA-1'
        body += '\n To change your alerts preferences please use the following link:\n' + u.link
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        #A.sendMail(test_name, u.email, body)
        # print(body)
        #A.addAlert(test_name, u.name, str(df_w.shape[0])+' users with huge walltime.')
else:
    print('No Alarm')


# <h2>Third Alarm</h2>
# <h3>Notify if user job efficiency drops before 70%</h3>

# In[10]:


s = {
    "size": 0,  # get one job entry only for debugging purposes
    'query': {
        'bool': {
            'must': [
                {"term": {"prodsourcelabel": "user"}},
                {'range': {
                    'modificationtime': {
                        "gte": "now-1d",
                        "lt":  "now"}
                }
                },
                {'bool': {
                    'must_not': [
                        {"term": {"produsername": "gangarbt"}},
                        {"term": {"processingtype": "pmerge"}},
                        {"term": {"jobstatus": "cancelled"}},
                        {"term": {"jobstatus": "closed"}}
                    ]
                }
                }
            ],
        }
    },
    "aggs": {
        "status": {
            "terms": {
                "field": "jobstatus",
                "order": {"corecount_sum": "desc"},
                "size": 5
            },
            "aggs": {
                "corecount_sum": {
                    "sum": {"field": "actualcorecount"}
                },
            }
        }
    }
}


res = es.search(index=ind, body=s)
# print(res)

agg = res['aggregations']['status']['buckets']
# print(agg)

# create df
df_e = json_normalize(agg)

finished = df_e[df_e['key'] == 'finished']
successful = finished['corecount_sum.value'].iloc[0]
failed = df_e[df_e['key'] == 'failed']
total = failed['corecount_sum.value'].iloc[0] + successful


LIMIT_EFFICIENCY = 0.7
Alarm = ''
if (total == 0):
    Alarm = "Alarm, no finished user jobs in last 24 hours"
else:
    efficiency = successful / total
    print(str(efficiency))
    if (efficiency < LIMIT_EFFICIENCY):
        Alarm = "Alarm, user job efficiency is " + str(round(efficiency, 1))

if (len(Alarm) > 0):
    print(Alarm)


if (len(Alarm) > 0):
    test_name = 'Top Analysis users [Low efficiency]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name + ',\n\n'
        body += 'the following alarm was raised regarding the global user job efficiency in the last 24 hours:\n\n'
        body += Alarm + '\n'
        body += '\n The efficiency is defined as walltime of successful jobs divided by the walltime of successful plus failed jobs'
        body += '\n The efficiency is calculated on all user jobs in the last 24 hours.'
        body += '\n To get more information about this alert message and its interpretation, please visit:\n'
        body += 'https://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis'
        body += '\nhttps://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis-User'
        body += '\n To change your alerts preferences please use the following link:\n' + u.link
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        A.sendGunMail(test_name, u.email, body)
        # print(body)
        A.addAlert(test_name, u.name, Alarm)
else:
    print('No Alarm')


# <h2>Fourth alarm -- DISABLED --- TO BE REVIEWED</h2>
# <h3>get name of users with >70 retries in last 24 hours, should we also add a lower limit on the number of jobs?</h3>


s = {
    "size": 0,  # get one job entry only for debugging purposes
    'query': {
        'bool': {
            'must': [
                {"term": {"prodsourcelabel": "user"}},  # add jobstatus failed
                {"term": {"jobstatus": "failed"}},
                {'range': {
                    'modificationtime': {
                        "gte": "now-1d",
                        "lt":  "now"}
                }},
                {'range': {
                    'attemptnr': {
                        "gte": "999",  # "70",
                        "lt":  "1000"},

                }},
                {'bool': {
                    'must_not': [
                        {"term": {"produsername": "gangarbt"}},
                        {"term": {"processingtype": "pmerge"}},
                    ]
                }
                }
            ],
        }
    },
    "aggs": {
        "status": {
            "terms": {
                "field": "produsername",
                "order": {"corecount_sum": "desc"},
                "size": 5
            },
            "aggs": {
                "corecount_sum": {
                    "sum": {"field": "actualcorecount"}
                },
            }
        }
    }
}

res = es.search(index=ind, body=s)
# print(res)

agg = res['aggregations']['status']['buckets']
# print(agg)

# create df
df_a = json_normalize(agg)
if df_a.shape[0] > 0:
    df_a = df_a.drop("doc_count", 1)

    # LIMIT_JOBS = 5 #for testing
    #df_a = df_a[df_a["corecount_sum.value"] > LIMIT_JOBS]

    df_a.columns = ['jobs', 'user']
    print(df_a.to_string())


if df_a.shape[0] > 0:
    print('here')
    test_name = 'Top Analysis users [Retrial attempts]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name + ',\n\n'
        body += 'the following users have jobs with more than 70 retrials in the last 24 hours:\n\n'
        body += df_a.to_string() + '\n'
        body += '\n To get more information about what each user is doing, please visit:\n'
        for i in df_a['user'].iteritems():
            body += 'https://bigpanda.cern.ch/tasks/?username=' + \
                str(i[1]) + '\n'
        body += '\n If deemed necessary, please contact the user to ask what he/she is doing:\n'
        body += '\nhttps://its.cern.ch/jira/browse/ADCDPA-1'
        body += '\n To change your alerts preferences please use the following link:\n' + u.link
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        #A.sendMail(test_name, u.email, body)
        # print(body)
        #A.addAlert(test_name, u.name, str(df_a.shape[0])+' users with jobs with large retrial attempts.')
else:
    print('No Alarm')
