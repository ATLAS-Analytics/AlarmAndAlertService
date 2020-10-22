# Deduplicate PerfSONAR data in all the indices where _id is not autogenerated
#
# it accepts a date range.
import sys
import json
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import scan, bulk
from datetime import datetime

with open('/config/config.json') as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

start_int = '2020-10-01 01:00'
stop_int = '2020-10-01 02:00'
start_dt = datetime.strptime(start_int, '%Y-%m-%d %H:%M')
stop_dt = datetime.strptime(stop_int, '%Y-%m-%d %H:%M')
print('start:', start_dt, '\nstop: ', stop_dt)

# ### define what are the indices to deduplicate

ps_indices = [
    # 'ps_meta',
    'ps_owd',
    'ps_packetloss',
    'ps_retransmits',
    # 'ps_status',
    'ps_throughput',
    'ps_trace'
]


def exec_delete(fdata):
    data = []
    for did, index in fdata:
        data.append({
            '_op_type': 'delete',
            '_index': index,
            '_id': did
        })
    # print(data)
    res = bulk(client=es, actions=data, stats_only=True, timeout="5m")
    print("deleted:", res[0], "  issues:", res[1])
    return


for ind in ps_indices:
    print("Checking: ", ind)

    query = {
        "_source": False,
        "query": {
            "range": {
                "timestamp": {
                    "gt": int(start_dt.timestamp()*1000),
                    "lte": int(stop_dt.timestamp()*1000)
                }
            }
        }
    }

    ids = {}
    docs_read = 0
    scroll = scan(client=es, index=ind, query=query, timeout="5m")
    for res in scroll:
        docs_read += 1
        if not docs_read % 10000:
            print('docs read', docs_read)
        if res['_id'] in ids:
            ids[res['_id']].append(res['_index'])
        else:
            ids[res['_id']] = [res['_index']]

    print('read:', docs_read)

    toDelete = []
    docs_deleted = 0
    for did, indices in ids.items():
        if len(indices) == 1:
            continue
        elif len(indices) == 2:
            toDelete.append((did, indices[1]))
        else:
            print('Huston we have a problem')
            print('did:', did, 'indices', indices)
            break

    print('to delete:', len(toDelete))
    batch = 100
    for i in range(int(len(toDelete)/batch)+1):
        exec_delete(toDelete[i*batch:(i+1)*batch])
