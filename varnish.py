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
from datetime import datetime
import requests
from alerts import alarms
from elasticsearch import Elasticsearch
from typing import Any, Dict


# load ES_HOST, ES_USER, ES_PASS from environment
import os
env = {}
for var in ['ES_HOST', 'ES_USER', 'ES_PASS']:
    env[var] = os.environ.get(var, None)
    if not env[var]:
        print('environment variable {} not set!'.format(var))
        sys.exit(1)

es = Elasticsearch(
    hosts=[{'host': env['ES_HOST'], 'port': 9200, 'scheme': 'https'}],
    basic_auth=(env['ES_USER'], env['ES_PASS']),
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

# this function loads mapping of varnish instances from github config file

URL = "https://raw.githubusercontent.com/ivukotic/v4A/refs/heads/frontier/configurations/configurations.json"


def add_missing_defaults(parent, child):
    for key in ['port', 'type', 'file', 'active', 'local', 'in_CDN', 'responsible']:
        if key not in child:
            child[key] = parent.get(key)


def denormalize_configurations(d: Dict[str, Any]) -> Dict[str, Any]:
    for site in d.get('sites', []):
        add_missing_defaults(d, site)
        for instance in site.get('instances', []):
            add_missing_defaults(site, instance)
    return d


def load_configurations() -> Dict[str, Any]:
    resp = requests.get(URL, timeout=15)
    resp.raise_for_status()          # fail fast if the download didn’t work
    data: Any = resp.json()
    if not isinstance(data, dict):
        raise ValueError(
            "Expected a top-level JSON object (dict), got something else!")
    return denormalize_configurations(data)


cs = load_configurations()
print(f"Loaded {len(cs['sites'])} endpoint definitions")

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
liveness_data = check_varnish_liveness()
# print(liveness_data)

# loop over endpoints and test ones that have active: true
for site in cs['sites']:
    # print(site)
    if site['active'] is False:
        print(f"Skipping inactive site {site.get('name')}")
        continue
    if site['type'] != 'frontier':
        print(f"Skipping non-Frontier site {site.get('name')}")
        continue
    found = False
    for bucket in liveness_data:
        if bucket['key'] == site['url']:
            found = True
            total = bucket['doc_count']
            site["liveness_tests"] = total
            status_200 = bucket['status_200']['doc_count']
            site["liveness_status_200"] = status_200
            # print(site)
            print(
                f"Endpoint: {site['url']}, Total: {total}, Status 200: {status_200}, site: {site['name']}, admin: {site['responsible']['name']}")
            if total == 0:
                print(f'should not happen - endpoint {site["url"]}.')
                break
            if status_200 == 0 or (status_200 / total) < 0.9:
                print(
                    f'ALARM: Varnish endpoint {site["url"]} had less than 90% status 200 responses.')
            #     ALARM = alarms('Analytics', 'Varnish', 'liveness')
            #     ALARM.addAlarm(
            #         body=f'Varnish endpoint {endpoint["url"]} had less than 90% status 200 responses.',
            #         tags=['liveness'],
            #         source={'varnish_server': endpoint['url'],
            #                 'site': endpoint['site'], 'admin': endpoint['responsible']['email']}
            #     )
            break
    if not found:
        print(f'No data found for endpoint {site["url"]}.')

# this function queries ES.
# input parameters are startTime and endTime (in milliseconds since epoch).
# it gets:
# * min and max number of requests (MAIN.client_req),
# * min and max number of hits (MAIN.cache_hit),
# * min and max number of cache misses (MAIN.cache_miss)
# * min and max number of cache expulsions (MAIN.n_lru_nuked)
# in last 24 hours, and previous 24 hours for each varnish instance.
# the data is in index named "varnish" and should be grouped by "site" and "instance" field.


def get_varnish_monitoring_data(startTime, endTime):
    query = {
        "bool": {
            "must": [
                {"range": {"@timestamp": {
                    "gte": startTime, "lte": endTime}}},
            ]
        }
    }

    aggs = {
        "by_site": {
            "terms": {"field": "site", "size": 1000},
            "aggs": {
                "by_instance": {
                    "terms": {"field": "instance", "size": 1000},
                    "aggs": {
                        "min_requests": {"min": {"field": "MAIN.client_req"}},
                        "max_requests": {"max": {"field": "MAIN.client_req"}},
                        "min_hits": {"min": {"field": "MAIN.cache_hit"}},
                        "max_hits": {"max": {"field": "MAIN.cache_hit"}},
                        "min_misses": {"min": {"field": "MAIN.cache_miss"}},
                        "max_misses": {"max": {"field": "MAIN.cache_miss"}},
                        "min_expulsions": {"min": {"field": "MAIN.n_lru_nuked"}},
                        "max_expulsions": {"max": {"field": "MAIN.n_lru_nuked"}},
                    }
                }
            }
        }
    }

    res = es.search(
        index='varnish', size=0, query=query, aggs=aggs)

    return res['aggregations']['by_site']['buckets']


print('Checking Varnish monitoring data...')
buckets = get_varnish_monitoring_data(startTime, currentTime)

# for site_bucket in buckets:
#     site = site_bucket['key']
#     for instance_bucket in site_bucket['by_instance']['buckets']:
#         instance = instance_bucket['key']
#         min_requests = instance_bucket['min_requests']['value']
#         max_requests = instance_bucket['max_requests']['value']
#         min_hits = instance_bucket['min_hits']['value']
#         max_hits = instance_bucket['max_hits']['value']
#         min_misses = instance_bucket['min_misses']['value']
#         max_misses = instance_bucket['max_misses']['value']
#         min_expulsions = instance_bucket['min_expulsions']['value']
#         max_expulsions = instance_bucket['max_expulsions']['value']

#         # Here you can add logic to compare these values with previous period
#         # and create alarms if needed.
#         print(
#             f"Site: {site}, Instance: {instance}, Requests: {max_requests-min_requests}, Hits: {max_hits-min_hits}, Misses: {max_misses-min_misses}, Expulsions: {max_expulsions-min_expulsions}"
#         )

# ALARM = alarms('Analytics', 'Varnish', 'monitoring')

# res = es.count(index='varnish', query=query)
# print(res)
# if res['count'] == 0:
#     ALARM.addAlarm(body='Issue in indexing tasks.', tags=['tasks'])


print('Done.')
