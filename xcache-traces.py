# continuously looks up Rucio traces from UC ES,
# filters bad transfers through xcaches
# retries and classifies them
# ====

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
nproc = 5
procs = []


def splitURL(url):
    originStart = url.index('root:', 5)
    cache = url[:originStart-2]
    op = url[originStart:]

    pathStart = op.index('1094/', 8)+4
    origin = op[:pathStart]
    opath = op[pathStart:]
    return cache, origin, opath


def stater(i, q, r):
    while not q.empty():
        doc = q.get()
        c, o, p = splitURL(doc['url'])
        print(f'thr:{i}, checking cache {c} origin {o} for {p}')
        myclient = client.FileSystem(o)
        try:
            myclient = client.FileSystem(o)
            status, statInfo = myclient.stat(p, timeout=5)
            print(status)  # , statInfo)
            doc['ok'] = status.ok
            doc['error'] = status.error
            doc['fatal'] = status.fatal
            doc['message'] = status.message
            doc['status'] = status.status
            doc['code'] = status.code
            doc['_index'] = "remote_io_retries"
            doc['timestamp'] = int(time.time()*1000)
        except Exception as e:
            print('issue stating file.', e)

        if not status.ok:
            r.put(doc, block=True, timeout=0.1)
            continue

        try:
            with client.File() as f:
                print("opening:", o+p)
                ostatus, nothing = f.open(o+p, timeout=5)
                print('open: ', ostatus)
                doc['open_ok'] = ostatus.ok
                doc['open_error'] = ostatus.error
                doc['open_fatal'] = ostatus.fatal
                doc['open_message'] = ostatus.message
                doc['open_status'] = ostatus.status
                doc['open_code'] = ostatus.code
                if ostatus.ok:
                    rstatus, data = f.read(offset=0, size=1024, timeout=10)
                    print("reading", rstatus)
                    doc['read_ok'] = rstatus.ok
                    doc['read_error'] = rstatus.error
                    doc['read_fatal'] = rstatus.fatal
                    doc['read_message'] = rstatus.message
                    doc['read_status'] = rstatus.status
                    doc['read_code'] = rstatus.code
                    # f.close(timeout=10) # not needed
        except Exception as e:
            print('issue reading file.', e)

        r.put(doc, block=True, timeout=0.1)

    print('Thread done.')
    print("===>", q.qsize())


def store(q, r):
    print("storring results.")
    allDocs = []
    while not q.empty() and not r.empty():
        while not r.empty():
            doc = r.get()
            allDocs.append(doc)
        print('received results:', len(allDocs))
        time.sleep(5)

    try:
        print('storing results in ES.')
        res = helpers.bulk(es, allDocs, raise_on_exception=True)
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
    print('done storing.')


if __name__ == "__main__":

    with open('/config/config.json') as json_data:
        config = json.load(json_data,)

    es = Elasticsearch(
        hosts=[{'host': config['ES_HOST'], 'port':9200, 'scheme':'https'}],
        basic_auth=(config['ES_USER'], config['ES_PASS']))
    es.options(request_timeout=60)

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

    q = Queue()
    for i in range(results):
        doc = res['hits']['hits'][i]['_source']
        ndoc = {k: doc[k] for k in keep}
        # print(ndoc)
        q.put(ndoc)

    r = Queue()
    for i in range(nproc):
        p = Process(target=stater, args=(i, q, r))
        p.start()
        procs.append(p)

    p = Process(target=store, args=(q, r))
    p.start()
    procs.append(p)
    for i in range(nproc+1):
        procs[i].join()

    print("Done testing.")

# if len(tkids) > 0:
#     ALARM = alarms('Analytics', 'Frontier', 'Bad SQL queries')
#     ALARM.addAlarm(
#         body='Bad SQL queries',
#         source={'users': list(users), 'tkids': list(tkids)}
#     )
