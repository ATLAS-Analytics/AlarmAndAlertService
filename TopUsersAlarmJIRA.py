#%% [markdown]
# <h1>This notebook retrieves from ES the info from jobs_archive about 10 top users, and sends alarm if usage is above certain thresholds</h1>

#%%
import numpy as np
import re
import subprocess
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from pandas.io.json import json_normalize
from IPython.display import display
from pandas import DataFrame
import pandas as pd
from datetime import datetime, timedelta
import datetime

import json

#%% [markdown]
# <h2>Retrieve all job indexes from ES</h2>

#%%
#define function to filter on time
def time_filter(indices, last_days=1, pattern=''):
    if last_days == 0:
        return ["jobs_archive_*"]
    filtered = []
    if pattern:
        for i in indices:
            if pattern in i:
                filtered.append(i.rstrip())
        return filtered
    today = datetime.date.today()
    filtered = []
    datefmt = '%Y-%m-%d'
    for i in indices:
        day = re.sub(r'jobs_archive_', '', i).rstrip()
        #print(day)
        if '_reindexed' in day:
            day = re.sub(r'_reindexed', '', day).lstrip()        
        day = datetime.datetime.strptime(day, datefmt).date()
        diff = today - day
        if diff.days < last_days:
            filtered.append(i.rstrip())
    return filtered


#%%

with open('/config/config.json') as json_data:
    config = json.load(json_data,)

# ## Establish Elasticsearch connection
es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'schema':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)

#get job archive indices from ES
indices = es.cat.indices(index="jobs_archive_*", h="index", request_timeout=600).split('\n')
indices = sorted(indices)
indices = [x for x in indices if x != '']
if 'jobs_archive_2016_status' in indices:
    indices.remove('jobs_archive_2016_status')

#%% [markdown]
# <h2>Retrieve job archives of interest from ES</h2>

#%%
# retrieve job info from last 2 days
# use e.g. last_days=7 or pattern='2016-02' (no wildcard !)
NDAYS=2  #NDAYS=150 #NDAYS=''
PATTERN='' #PATTERN = '2016-03' #PATTERN=''
ind = time_filter(indices, last_days=NDAYS, pattern=PATTERN)
ind = ','.join(ind)
print(ind)

#%% [markdown]
# ## Alerts and Alarms

#%%
from subscribers import subscribers
import alerts

S = subscribers()
A = alerts.alerts()

#%% [markdown]
# <h2>First Alarm</h2> 
# <h3>get top 10 users/24 hours for walltime*core, and filter out sum walltime > 15 years</h3>
# <h3>convert walltime in number of cores used per day, by assuming all jobs are single core</h3>

#%%
s = {
    "size": 0, 
    'query':{
        'bool':{
            'must':[
                { "term": {"prodsourcelabel":"user" } },
                { 'range' : {
                    'modificationtime' : {
                        "gte" : "now-1d",
                        "lt" :  "now"}
                    }                
                },
                { 'bool' : {
                    'must_not':[
                        { "term": {"produsername": "gangarbt" } },
                        { "term": {"processingtype":"pmerge" } } ,
                        { 'exists' : { "field" : "workinggroup" }}    # only users without workinggroup priviledges
                        ]                        
                    }
                }
            ],
        }
    },
    "aggs": {
        "users":{
            "terms": { 
                "field": "produsername", 
                "order": {"walltime_core_sum": "desc"},
                "size": 10
            },
            "aggs": {
                "walltime_core_sum": {
                    "sum": {
                        "script" : {   # use scripted field to calculate corecount
                            "inline": "def core=doc['actualcorecount'].value; if (core!=null) {return doc['wall_time'].value * core} else {return doc['wall_time'].value}"
                        }
                    } 
                    
                },
            }
        }
    }
}

res = es.search(index=ind, body=s, request_timeout=12000)
#print(res) 

agg = res['aggregations']['users']['buckets']
jsondata = json.dumps(agg)

process = subprocess.Popen(["curl", "-D-", "-H", "Content-Type:application/json" ,"-X","POST","--data", jsondata, "http://test-jgarcian.web.cern.ch/test-jgarcian/cgi-bin/usersJIRA.py"],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()
#print(agg)

#create df
df_w = json_normalize(agg)
df_w['walltime_core_sum.value'] = df_w['walltime_core_sum.value'].apply(lambda x: timedelta(seconds=int(x)).days/365.2)
df_w['ncores']=df_w['walltime_core_sum.value'].apply(lambda x: x*365.) #transform walltime[year] in walltime[day]

LIMIT_WALLTIME = 15 # 5 for testing
df_w = df_w[df_w["walltime_core_sum.value"] > LIMIT_WALLTIME]

display(df_w)
df_w.columns = ['jobs', 'user', 'walltime used [years]', 'number of cores']
print(df_w.to_string())


#%%
if df_w.shape[0]>0:
    test_name='Top Analysis users [Large wall time]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name+',\n\n'
        body += 'the following users used substantial wall time (more than 15 years/last 24 hours, corresponding to 5475 cores/day):\n\n'
        body += df_w.to_string() + '\n'
        body += '\n To get more information about this alert message and its interpretation, please visit:\n'
        body += 'https://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis-User'
        body += '\nhttps://its.cern.ch/jira/browse/ADCDPA-1'
        body += '\n To change your alerts preferences please use the following link:\n'+u.link
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        #A.sendMail(test_name, u.email, body)
        #print(body)
    #A.addAlert(test_name, u.name, str(df_w.shape[0])+' users with huge walltime.')
else:
    print('No Alarm')

#%% [markdown]
# <h2>Second Alarm</h2> 
# <h3>get top 10 users/24 hours for inputfilebytes, and filter out sum input size > 500 TB</h3>

#%%
s = {
    "size": 0, # get one job entry only for debugging purposes    
    'query':{
        'bool':{
            'must':[
                { "term": {"prodsourcelabel":"user" } },
                { 'range' : {
                    'modificationtime' : {
                        "gte" : "now-1d",
                        "lt" :  "now"}
                    }                
                },
                { 'bool' : {
                    'must_not':[
                        { "term": {"produsername": "gangarbt" } },
                        { "term": {"processingtype":"pmerge" } },
                        { "term": {"jobstatus" :"closed"} },
                        { "term": {"jobstatus" :"cancelled"} },
                        { 'exists' : { "field" : "workinggroup" }}]                        
                    }
                }
            ],
        }
    },
    "aggs": {
        "users":{
            "terms": { 
                "field": "produsername", 
                "order": {"inputsize_sum": "desc"},
                "size": 10
            },
            "aggs": {
                "inputsize_sum": {
                    "sum": { "field": "inputfilebytes" }                     
                },
            }
        }
    }
}

res = es.search(index=ind, body=s, request_timeout=12000)
#print(res) 


agg = res['aggregations']['users']['buckets']
#print(agg)

jsondata = json.dumps(agg)

process = subprocess.Popen(["curl", "-D-", "-H", "Content-Type:application/json" ,"-X","POST","--data", jsondata, "http://test-jgarcian.web.cern.ch/test-jgarcian/cgi-bin/usersJIRA.py"],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()
print(stdout)

#create df
df_i = json_normalize(agg)
df_i['inputsize_sum.value'] = df_i['inputsize_sum.value'].apply(lambda x: x*0.00000000000089)

LIMIT_INPUTSIZE = 500 # 5 for testing
df_i = df_i[df_i["inputsize_sum.value"] > LIMIT_INPUTSIZE]
#display(df_i)

df_i.columns = ['jobs', 'input size [TB]', 'user']
print(df_i.to_string())


#%%
if df_i.shape[0]>0:
    test_name='Top Analysis users [Large input data size]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name+',\n\n'
        body += 'the following users processed rather substantial input data (>500 TB/last 24 hours):\n\n'
        body += df_i.to_string() + '\n'
        body += '\n To get more information about this alert message and its interpretation, please visit:\n'
        body += 'https://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis-User'
        body += '\nhttps://its.cern.ch/jira/browse/ADCDPA-1'
        body += '\n To change your alerts preferences please use the following link:\n'+u.link
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        #A.sendMail(test_name, u.email, body)
        #print(body)
        #A.addAlert(test_name, u.name, str(df_w.shape[0])+' users with huge walltime.')
else:
    print('No Alarm')        

#%% [markdown]
# <h2>Third Alarm</h2> 
# <h3>Notify if user job efficiency drops before 70%</h3>

#%%
s = {
    "size": 0, # get one job entry only for debugging purposes    
    'query':{
        'bool':{
            'must':[
                { "term": {"prodsourcelabel":"user" } },
                { 'range' : {
                    'modificationtime' : {
                        "gte" : "now-1d",
                        "lt" :  "now"}
                    }                
                },
                { 'bool' : {
                    'must_not':[
                        { "term": {"produsername": "gangarbt" } },
                        { "term": {"processingtype":"pmerge" } } ,
                        { "term": {"jobstatus" :"cancelled" } } ,
                        { "term": {"jobstatus" :"closed"}}
                        ]                        
                    }
                }
            ],
        }
    },
    "aggs": {
        "status":{
            "terms": { 
                "field": "jobstatus", 
                "order": {"corecount_sum": "desc"},
                "size": 5
            },
            "aggs": {
                "corecount_sum": {
                    "sum": { "field": "actualcorecount" }                     
                },
            }
        }
    }
}

            
res = es.search(index=ind, body=s, request_timeout=12000)
#print(res) 

agg = res['aggregations']['status']['buckets']
#print(agg)

#create df
df_e = json_normalize(agg)
#display(df_e)

finished = df_e[df_e['key']=='finished']
successful = finished['corecount_sum.value'].iloc[0]
failed = df_e[df_e['key']=='failed']
total = failed['corecount_sum.value'].iloc[0] + successful


LIMIT_EFFICIENCY = 0.7
Alarm = ''
if (total==0):
    Alarm = "Alarm, no finished user jobs in last 24 hours"
else:
    efficiency = successful/total
    print(str(efficiency))
    if (efficiency < LIMIT_EFFICIENCY):
        Alarm = "Alarm, user job efficiency is "+str(round(efficiency,1))    

if (len(Alarm)>0):
    print(Alarm)


#%%
if (len(Alarm)>0):
    test_name='Top Analysis users [Low efficiency]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name+',\n\n'
        body += 'the following alarm was raised regarding the global user job efficiency in the last 24 hours:\n\n'
        body += Alarm + '\n'
        body += '\n The efficiency is defined as walltime of successful jobs divided by the walltime of successful plus failed jobs'
        body += '\n The efficiency is calculated on all user jobs in the last 24 hours.'
        body += '\n To get more information about this alert message and its interpretation, please visit:\n'
        body += 'https://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis'
        body += '\nhttp://atlas-kibana.mwt2.org:5601/app/kibana#/dashboard/FL-Analysis-User'
        body += '\n To change your alerts preferences please use the following link:\n'+u.link
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        A.sendMail(test_name, u.email, body)
        #print(body)
        A.addAlert(test_name, u.name, Alarm)
else:
    print('No Alarm') 

#%% [markdown]
# <h2>Fourth alarm</h2> 
# <h3>Users with large number of failing jobs (>1000) and retries (>1.5) </h3>

#%%
s = {
  "size": 0,
  "_source": {
    "excludes": []
  },
  "aggs": {
    "users": {
      "terms": {
        "field": "produsername",
        "size": 20,
        "order": {
          "_count": "desc"
        }
      },
      "aggs": {
        "cpuconsumptiontime": {
          "sum": {
            "field": "cpuconsumptiontime"
          }
        },
        "attemptnr": {
          "avg": {
            "field": "attemptnr"
          }
        },
        "jeditaskid": {
          "top_hits": {
            "docvalue_fields": [
              "jeditaskid"
            ],
            "_source": "jeditaskid",
            "size": 1,       
            "sort": [
              {
                "jeditaskid": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  },
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "(prodsourcelabel:user) AND (NOT produsername:\"gangarbt\") AND (NOT processingtype:pmerge)",
            "analyze_wildcard": "true",
            "lowercase_expanded_terms": "false"
          }
        },
        {
            
          "range": {
            'modificationtime' : {
                "gte" : "now-1d",
                "lt" :  "now"}
            } 
        }
      ],
      "filter": [],
      "should": [],
      "must_not": []
    }
  }
}


res = es.search(index=ind, body=s, request_timeout=12000)
#print(res) 

agg = res['aggregations']['users']['buckets']
#print(agg)

nagg = []
for x in agg :
    y = x
    if isinstance(y['jeditaskid'], dict):
        y['jeditaskid'] = x['jeditaskid']['hits']['hits'][0]['sort']
    elif isinstance(y['jeditaskid'], list):
        y['jeditaskid'] = x['jeditaskid'][0]
    else :
        y['jeditaskid'] = x['jeditaskid']
    nagg.append(y)

#create df
df_w = json_normalize(nagg)
df_w['cpuconsumptiontime.value'] = df_w['cpuconsumptiontime.value'].apply(lambda x: timedelta(seconds=int(x)).days/365.2)

LIMIT_FAILURES = 1000 # 5 for testing
LIMIT_ATTEMPT = 1.5 # 5 for testing

df_w = df_w[df_w["doc_count"] > LIMIT_FAILURES]
df_w = df_w[df_w["attemptnr.value"] > LIMIT_ATTEMPT]

display(df_w)
df_w.columns = ['avg attempt', 'cpusonsumption [years]', 'failed jobs', 'jeditaskid', 'User']
print(df_w.to_string())


#%%
if df_w.shape[0]>0:
    test_name='Top Analysis users [Retrial attempts]'
    for u in S.get_immediate_subscribers(test_name):
        body = 'Dear ' + u.name+',\n\n'
        body += 'the following users have tasks having troubles running:\n\n'
        body += df_w.to_string() + '\n\n'
        body += '\n\nBest regards,\nATLAS Alarm & Alert Service'
        print(u.name)
        A.sendMail(test_name, u.email, body)
        #print(body)
        A.addAlert(test_name, u.name, str(df_w.shape[0])+' large number of failures.')
else:
    print('No Alarm')


#%%



