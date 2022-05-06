# continuously looks up Rucio traces from UC ES,
# filters bad transfers through xcaches
# retries and classifies them
# ====


# needs a valid proxy.
# write status back to Elasticsearch
# TLS error: Unable to use CA cert directory /etc/grid-security/certificates; does not exist.

import sys
import datetime
import time
# from alerts import alarms
from elasticsearch import Elasticsearch, helpers, exceptions as es_exceptions
# from elasticsearch.helpers import scan
import json

from multiprocessing import Process, Queue

from XRootD import client

nhours = 1
nproc = 3
q = Queue()
r = Queue()
procs = []


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
        if doc == "stop":
            print(f'Thread {i}, Stopping.')
            break
        if not doc:
            time.sleep(10)
            continue
        c, o, p = splitURL(doc['url'])
        print(f'thr:{i}, checking cache {c} origin {o} for {p}')
        try:
            myclient = client.FileSystem(o)
            status, statInfo = myclient.stat(p, timeout=10)
            print(status)  # , statInfo)
            doc['ok'] = status.ok
            doc['error'] = status.error
            doc['fatal'] = status.fatal
            doc['message'] = status.message
            doc['status'] = status.status
            doc['code'] = status.code
            doc['_index'] = "xcache_retries",
            doc['timestamp'] = time.time()
            r.put(doc)

        except Exception as e:
            print('issue opening file.', e)


def store():
    print("storing results in ES.")
    allDocs = []
    while True:
        doc = q.get()
        if not doc:
            break
        allDocs.append(doc)
    try:
        res = helpers.bulk(es, allDocs,
                           raise_on_exception=True, request_timeout=60)
        print("inserted:", res[0], '\tErrors:', res[1])
    except es_exceptions.ConnectionError as e:
        print('ConnectionError ', e)
    except es_exceptions.TransportError as e:
        print('TransportError ', e)
    except helpers.BulkIndexError as e:
        print(e[0])
        for i in e[1]:
            print(i)
    except Exception as e:
        print('Something seriously wrong happened.', e)


for i in range(nproc):
    p = Process(target=tester, args=(i, q,))
    procs.append(p)
    p.start()


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

for i in range(nproc):
    q.put("stop")

for i in range(nproc):
    procs[i].join()

store()

# if len(tkids) > 0:
#     ALARM = alarms('Analytics', 'Frontier', 'Bad SQL queries')
#     ALARM.addAlarm(
#         body='Bad SQL queries',
#         source={'users': list(users), 'tkids': list(tkids)}
#     )
