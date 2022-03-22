import sys
import json
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import scan, bulk
from datetime import datetime
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

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

start_int = '2021-10-01 00:00'
stop_int = '2022-10-02 00:00'
start_dt = datetime.strptime(start_int, '%Y-%m-%d %H:%M')
stop_dt = datetime.strptime(stop_int, '%Y-%m-%d %H:%M')
print('start:', start_dt, '\nstop: ', stop_dt)

# ### define what are the indices to deduplicate

indices = 'task_parameters*'

query = {
    "_source": ['taskparams.excludedSite', 'taskparams.userName'],
    "query": {
        "range": {
            "creationdate": {
                "gt": int(start_dt.timestamp()*1000),
                "lte": int(stop_dt.timestamp()*1000)
            }
        }
    }
}

docs_read = 0
data = {}
scroll = scan(client=es, index=indices, query=query, timeout="5m")
for res in scroll:
    docs_read += 1
    if not docs_read % 500:
        print('docs read', docs_read)
    d = res['_source']['taskparams']
    if not 'userName' in d or not 'excludedSite' in d:
        continue
    uN = d['userName']
    eS = d['excludedSite']
    if not eS:
        continue
    if uN not in data:
        data[uN] = {'tasks': 0, 'sites excluded': 0}
    data[uN]['tasks'] += 1
    data[uN]['sites excluded'] += len(eS)
print(data)
df = pd.DataFrame(data).transpose()
df['ratio'] = df['sites excluded']/df.tasks
df.plot(kind="bar")
fig = matplotlib.pyplot.gcf()
fig.set_size_inches(6, 6)
plt.tight_layout()
plt.savefig('mayuko.png')

print('read:', docs_read)
