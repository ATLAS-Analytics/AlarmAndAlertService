# Checks if Varnish servers are working fine
# ====
# It loads list of Varnish servers from github config file
# check liveness of all instances that are actively tested and creates alarm if any of them is not available.
# checks monitoring data of each instance
# and creates alarm if:
# - no data received in last 1 day
# - there is a significant drop in requests in last 1 day compared to previous period
# - there is a significant difference in cache hit ratio in last 1 day compared to previous period
# - there was at least one restart in last 1 day

import sys
import json
from datetime import datetime
import requests
from alerts import alarms
from elasticsearch import Elasticsearch, exceptions as es_exceptions


config_path = '/config/config.json'
# config_path = 'kube/secrets/config.json'

with open(config_path) as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'port': 9200, 'scheme': 'https'}],
    basic_auth=(config['ES_USER'], config['ES_PASS']),
    request_timeout=60)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)


ct = datetime.now()
currentTime = int(round(datetime.now().timestamp() * 1000))
lastHours = 24
startTime = currentTime - lastHours * 3600000
print('start time', startTime)
print('current time', datetime.now())

# this function will load list of varnish endpoints from github config file


def load_varnish_endpoints():
    url = 'https://raw.githubusercontent.com/ivukotic/v4A/refs/heads/frontier/configurations/endpoints.json'
    response = requests.get(url)
    if response.status_code == 200:
        servers = response.json()
        return servers
    else:
        print('Failed to load varnish endpoints.')
        return []

# this function loads mapping of varnish instances from github config file


def load_varnish_instances():
    url = 'https://raw.githubusercontent.com/ivukotic/v4A/refs/heads/frontier/configurations/mapping.json'
    response = requests.get(url)
    if response.status_code == 200:
        instances = response.json()
        return instances
    else:
        print('Failed to load varnish instances.')
        return {}


endpoints = load_varnish_endpoints()
mapping = load_varnish_instances()
print('Loaded', len(endpoints), 'varnish endpoints.')
print('Loaded', len(mapping), 'varnish instances.')

# this function queries ES. it runs a count query against varnish_status index.
# it filters data in range: "range": {"modificationtime": {"gte": startTime, "lte": currentTime}}
# and "kind": "conditions"
# for each unique "address" it returns number of documents found and number of documents where status==200.


def check_varnish_liveness():
    query = {
        "bool": {
            "must": [
                {"range": {"@timestamp": {
                    "gte": startTime, "lte": currentTime}}},
                {"term": {"kind": "conditions"}}
            ]
        }
    }

    aggs = {
        "by_address": {
            "terms": {"field": "address", "size": 1000},
            "aggs": {
                "status_200": {
                    "filter": {"term": {"status": 200}}
                }
            }
        }
    }

    res = es.search(
        index='varnish_status', size=0, query=query, aggs=aggs)

    return res['aggregations']['by_address']['buckets']


print('Checking Varnish liveness...')
buckets = check_varnish_liveness()
# print(buckets)

for endpoint in endpoints:
    if endpoint["active"] is not True or endpoint["local"] is True:
        continue
    for bucket in buckets:
        if bucket['key'] == endpoint['url']:
            total = bucket['doc_count']
            status_200 = bucket['status_200']['doc_count']
            # print(endpoint)
            print(
                f"Endpoint: {endpoint['url']}, Total: {total}, Status 200: {status_200}, site: {endpoint['site']}, admin: {endpoint['responsible']['name']}")
            if total == 0:
                print(f'should not happen - endpoint {endpoint["url"]}.')
                break
            if status_200 == 0 or (status_200 / total) < 0.9:
                ALARM = alarms('Analytics', 'Varnish', 'liveness')
                ALARM.addAlarm(
                    body=f'Varnish endpoint {endpoint["url"]} had less than 90% status 200 responses.',
                    tags=['liveness'],
                    source={'varnish_server': endpoint['url'],
                            'site': endpoint['site'], 'admin': endpoint['responsible']['email']}
                )
            break

# res = es.count(index='varnish_status', query=query)
# print(res)
# if res['count'] == 0:
#     ALARM.addAlarm(body='Issue in indexing jobs.', tags=['jobs'])

# # monitoring data
# ALARM = alarms('Analytics', 'Varnish', 'monitoring')
# query = {
#     "range": {"modificationtime": {"gte": startTime, "lte": currentTime}}
# }

# res = es.count(index='varnish', query=query)
# print(res)
# if res['count'] == 0:
#     ALARM.addAlarm(body='Issue in indexing tasks.', tags=['tasks'])

print('Done.')
