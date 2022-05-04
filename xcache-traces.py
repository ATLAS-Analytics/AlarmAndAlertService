# continuously looks up Rucio traces from UC ES,
# filters bad transfers through xcaches
# retries and classifies them
# ====


#  needs a valid proxy.

import sys
import datetime
import time
from alerts import alarms
from elasticsearch import Elasticsearch
import json

from multiprocessing import Process, Queue

from XRootD import client

q = Queue()


def splitURL(url):
    originStart = url.index('root:', 5)
    cache = url[:originStart-2]
    op = url[originStart:]

    pathStart = op.index('1094/', 8)+4
    origin = op[:pathStart]
    opath = op[pathStart:]
    return cache, origin, opath


def tester(i, q):
    while True:
        doc = q.get()
        if not doc:
            time.sleep(10)
            continue
        c, o, p = splitURL(doc['url'])
        print(f'thr:{i}, checking cache {c} origin {o} for {p}')
        try:
            myclient = client.FileSystem(o)
            status, statInfo = myclient.stat(p, timeout=10)
            print(status, statInfo)
        except Exception as e:
            print('issue opening file.', e)


for i in range(3):
    Process(target=tester, args=(i, q,)).start()


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
curtime = ct.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

td = datetime.timedelta(hours=nhours)
st = ct - td
starttime = st.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'

print('start time', starttime)
print('current time', curtime)

my_query = {
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

res = es.search(index='rucio_traces', query=my_query, size=10000)
results = res['hits']['total']['value']
print('total results:', results)

keep = [
    'stateReason', 'scope', 'filename', 'eventType', 'localSite',
    'dataset', 'filesize', 'timeStart', 'hostname', 'taskid', 'url', 'remoteSite', 'pq'
]

for i in range(results):
    doc = res['hits']['hits'][i]['_source']
    ndoc = {k: doc[k] for k in keep}
    # print(ndoc)
    q.put(ndoc)


# if len(tkids) > 0:
#     ALARM = alarms('Analytics', 'Frontier', 'Bad SQL queries')
#     ALARM.addAlarm(
#         body='Bad SQL queries',
#         source={'users': list(users), 'tkids': list(tkids)}
#     )
