# # Check if the packet loss data is abnormal

# This notebook finds out all the links which have at least five packet loss measurements in the past one hour and the average value of the packet loss measurements is greater than 2%. It is run by a cron job every hour, and it will write the detailed information of every alarm into Elastic Search with the _index: alarms-year-month and _type: packetloss.

import sys
import time
import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions, helpers


import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)

# ### If needed to calculate packet loss for other time moment than "now", overwrite it bellow

cdt = datetime.datetime.utcnow()
#cdt = datetime.datetime(2017,1,21,9,0,0)

GT = int((time.time() - 3 * 3600) * 1000)
# (cdt - datetime.timedelta(hours=3)).strftime("%Y%m%dT%H%m%S+0000")
LT = int(time.time() * 1000)
# LT = cdt.strftime("%Y%m%dT%H%m%S+0000")
print('between: ', GT, ' and ', LT)


# ### establish the Elastic Search connection


es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'schema':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)

# ### define functions to write an alarm record into ES with detailed info

ipSite = {}  # for mapping IP to Site name
toAlertOn = []


def generate_doc(src_site_ip, dest_site_ip, measurements, avgpl):
    if src_site_ip not in ipSite:
        print('serious source mapping issue')
        return
    if dest_site_ip not in ipSite:
        print('serious destination mapping issue')
        return

    doc = {
        '_index': get_index_name(),
        '_type': 'doc',
        'type': 'packetloss',
        'src': src_site_ip,
        'dest': dest_site_ip,
        'srcSite': ipSite[src_site_ip],
        'destSite': ipSite[dest_site_ip],
        'alarmTime': int((cdt - datetime.datetime(1970, 1, 1)).total_seconds() * 1000),
        'measurements': measurements,
        'packetLossAvg': avgpl
    }
    return doc


def get_index_name():
    date = cdt.strftime("%Y-%m")   # date format is yyyy-mm
    index_name = 'alarms-' + date
    return index_name


# ### get aggregated data for the past 2 hours
# This query is composed of 3 parts: a) filter - takes only packet loss data, and production servers in last 1h. b) aggregation -  finds average packet loss per source and destination c) finds IP to site name mapping (both source and destination)

query = {
    "size": 0,
    "query": {
        "bool": {
            "must": [
                {"term": {"src_production": True}},
                {"term": {"dest_production": True}}
            ],
            "filter": {
                "range": {
                    "timestamp": {"gt": GT, "lt": LT}
                }
            }
        }
    },
    "aggs": {
        "src": {
            "terms": {"field": "src", "size": 1000},
            "aggs": {
                "dest": {
                    "terms": {"field": "dest", "size": 1000},
                    "aggs": {
                        "avgpl": {
                            "avg": {
                                "field": "packet_loss"
                            }
                        }
                    }
                }
            }
        },
        "srcSites": {
            "terms": {"field": "src", "size": 1000},
            "aggs": {
                "srcsitename": {
                    "terms": {"field": "src_site"}
                }
            }
        },
        "destSites": {
            "terms": {"field": "dest", "size": 1000},
            "aggs": {
                "destsitename": {
                    "terms": {"field": "dest_site"}
                }
            }
        }
    }
}

# execute query
res = es.search(index="ps_packet_loss", body=query, request_timeout=120)
# print(res)


# ### proces IP to site name mapping data


srcsites = res['aggregations']['srcSites']['buckets']
print(srcsites)
for sS in srcsites:
    # print(sS)
    siteName = sS['srcsitename']['buckets']
    if len(siteName) == 0:
        siteName = 'UnknownSite'
    else:
        siteName = siteName[0]['key']
    ipSite[sS['key']] = siteName

destsites = res['aggregations']['destSites']['buckets']
# print(destsites)
for dS in destsites:
    # print(dS)
    siteName = dS['destsitename']['buckets']
    if len(siteName) == 0:
        siteName = 'UnknownSite'
    else:
        siteName = siteName[0]['key']
    ipSite[dS['key']] = siteName

print(ipSite)


# ### process packet loss averages


src = res['aggregations']['src']['buckets']
# print(src)

for s in src:
    # print(s)
    source = s['key']
    for d in s['dest']['buckets']:
        destination = d['key']
        avgpl = d['avgpl']['value']
        docs = d['doc_count']
#      print(source, destination, docs, avgpl)
        if avgpl > 0.02 and docs > 4:
            toAlertOn.append(generate_doc(source, destination, docs, avgpl))

for alert in toAlertOn:
    print(alert)


# ### write alarms to Elasticsearch

try:
    res = helpers.bulk(es, toAlertOn, raise_on_exception=True, request_timeout=60)
    print("inserted:", res[0], '\tErrors:', res[1])
except es_exceptions.ConnectionError as e:
    print('ConnectionError ', e)
except es_exceptions.TransportError as e:
    print('TransportError ', e)
except helpers.BulkIndexError as e:
    print(e[0])
    for i in e[1]:
        print(i)
except:
    print('Something seriously wrong happened.')
