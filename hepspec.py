import os
import sys
import json
import requests
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from grafana_api.grafana_face import GrafanaFace
from datetime import datetime, UTC
from elasticsearch.helpers import bulk, BulkIndexError
from itertools import chain
import copy
from typing import List, Tuple, Dict

panda_index = 'jobs'
hepspec_index = 'hepspec'

env = {}
for var in ['ES_HOST', 'HEPSPEC_ES_USER', 'HEPSPEC_ES_PASS', 'HEPSPEC_TOKEN']:
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


headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {env["HEPSPEC_TOKEN"]}',
    'Content-Type': 'application/x-ndjson'
}

lastDate_config = {
    "days": 1,
}

atlas_raw_query = {
    "query": {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "processingtype": {
                                        "value": "gangarobot-hepscore"
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                },
                {
                    "range": {
                        "modificationtime": {
                            "gte": "gte_replace",
                            "lte": "lte_replace"
                        }
                    }
                },
            ]
        }}
}

grafana_url = "https://monit-grafana.cern.ch/api/datasources/proxy/10213/_msearch?max_concurrent_shard_requests=5"
grafana_raw_query = """
            {\"search_type\":\"query_then_fetch\",\"ignore_unavailable\":true,\"index\":\"bmk-wlcg-prod-hepscore-*3.*,bmk-wlcg-prod-hepscore-v2.*\"}
            {\"size\":10000,\"query\":{\"bool\":{\"filter\":[{\"range\":{\"message._timestamp\":{\"gte\":gte_replace,\"lte\":lte_replace,\"format\":\"epoch_millis\
"}}},{\"query_string\":{\"analyze_wildcard\":true,\"query\":\" message.host.tags.purpose: \\\"ATLAS tests\\\"\"}}]}},\"sort\":[{\"message._timestamp\":{\"order\":\
"desc\",\"unmapped_type\":\"boolean\"}},{\"_doc\":{\"order\":\"desc\"}}],\"script_fields\":{}}
            """


def get_time_range_until_now(months: int = 0, days: int = 0, hours: int = 0) -> Tuple:
    """
    Get a tuple with start time and current time timestamp (int) range to query (startTime, currentTime).
    """
    lastMonths = months
    lastDays = days + lastMonths * 30
    lastHours = hours + lastDays * 24

    currentTime = int(round(datetime.now(UTC).timestamp() * 1000))
    startTime = currentTime - lastHours * 3600000

    return (startTime, currentTime)


def prepare_grafana_query(query: str = None, startTime: int = None, currentTime: int = None) -> str:
    """
    Replace placeholders in grafana query with given timestamps and return prepared query.
    """
    query = query.replace("gte_replace", str(startTime))
    query = query.replace("lte_replace", str(currentTime))
    query = query + '\n'

    return query


def prepare_atlas_query(query: dict = None, startTime: int = None, currentTime: int = None) -> dict:
    """
    Replace placeholders in atlas query with given timestamps and return prepared query.
    """
    query["query"]["bool"]["filter"][1]["range"]["modificationtime"]["gte"] = startTime
    query["query"]["bool"]["filter"][1]["range"]["modificationtime"]["lte"] = currentTime

    return query


def get_atlas_data(index: str = None, query: str = None):
    """
    Get ATLAS data from Elasticsearch based on given query.
    """
    bulk_data = []
    for hit in scan(es, query=query, index=panda_index):
        doc = {
            '_index': hepspec_index,
            '_id': hit['_id'],
            '_source': hit['_source'],
            '@timestamp': hit['_source']['modificationtime']
        }
        bulk_data.append(doc)
    print(f"loaded {len(bulk_data)} ATLAS jobs")
    return bulk_data


def get_grafana_data(url: str = None, query: str = None) -> List:
    """
    Get data from Grafana based on given url and query.
    """
    grafana_data = []

    response = requests.post(url, headers=headers, data=query)
    content = json.loads(response.content)
    data = content['responses'][0]['hits']['hits']
    grafana_data.append(data)

    grafana_combined_data = list(chain(*grafana_data))

    for doc in grafana_combined_data:
        doc['_source']['benchmarking'] = doc['_source'].pop('message')
        doc['_source']['benchmarking'].pop('_timestamp')
        doc['_source']['benchmarking'].pop('_timestamp_end')
        doc['_source']['benchmarking'].pop('_id')
        doc['_source']['benchmarking'].pop('json_version')
        hepscore_data = doc['_source']['benchmarking']['profiles'].pop(
            'hepscore', {})  # Extract hepscore data
        doc['_source']['benchmarking']['hepscore'] = hepscore_data
        doc['_source']['benchmarking']['hepscore'].pop('benchmarks')

        if 'plugins' in doc['_source']['benchmarking'].keys():
            doc['_source']['benchmarking'].pop('plugins')

    return grafana_combined_data


def process_grafana_data(grafana_data: List = None) -> Dict:
    grafana_processed_data = [{
        '_id': doc['_source']['benchmarking']['host']['tags']['jobid'],
        '_index': hepspec_index,
        '_source': doc['_source']} for doc in grafana_data]

    # Create a dictionary to store the count and records for each _id
    id_info = defaultdict(list)

    for data in grafana_processed_data:
        _id = data['_id']
        id_info[_id].append(data)

    # Print the duplicates and their counts
    for _id, records in id_info.items():
        count = len(records)
        if count > 1:
            print(f"Duplicate _id: {_id}, Count: {count}")

    # Filter the records to keep only the one with the latest @timestamp for each _id
    filtered_data = []

    for _id, records in id_info.items():
        records.sort(key=lambda x: x['_source']['@timestamp'], reverse=True)
        filtered_data.append(records[0])

    grafana_id_to_source_mapping = {data['_id']: data['_source']
                                    for data in filtered_data}

    try:
        unique_data_count = len(
            set(data['_source']['benchmarking']['host']['tags']['jobid'] for data in grafana_data))
        mapping_length = len(grafana_id_to_source_mapping)
        if unique_data_count == mapping_length:
            print(f"Grafana data correct, {mapping_length} documents.")
            return grafana_id_to_source_mapping
        else:
            raise NotFoundError(
                f"Number of unique _id values - {unique_data_count} is different from processed data - {mapping_length}")
    except NotFoundError as e:
        print(f"Error: {e}")
        # Handle the error here, for example, exit the script
        exit(1)


def prepare_es_actions(atlas_data: List = None, grafana_dict: Dict = None) -> List:
    """
    Prepare Elasticsearch actions to update hepspec index.
    """
    combined_data = copy.deepcopy(atlas_data)
    grafana_update = 0

    for data in combined_data:
        doc_id = data['_id']
        if doc_id in grafana_dict:
            # Update the dictionary with data from the second list
            data['_source'].update(grafana_dict[doc_id])
            grafana_update += 1

    print(
        f"Data to be updated in ES based on ATLAS: {len(combined_data)} documents")
    print(f"Combined atlas with benchmarking data: {grafana_update} documents")
    # Prepare actions for bulk indexing
    actions = [
        {
            "_index": hepspec_index,
            "_id": doc['_id'],
            "_source": doc['_source']
        }
        for doc in combined_data
    ]
    return actions


def main():

    grafana_connection = GrafanaFace(
        auth=config["HEPSPEC_TOKEN"], host=grafana_url)

    if es.indices.exists(index=hepspec_index):
        print(f"Index '{hepspec_index}' exists.")
    else:
        print(f"Index '{hepspec_index}' not found.")
        sys.exit(1)

    startTime, currentTime = get_time_range_until_now(
        days=lastDate_config["days"])

    atlas_query = prepare_atlas_query(
        query=atlas_raw_query, startTime=startTime, currentTime=currentTime)
    grafana_query = prepare_grafana_query(
        query=grafana_raw_query, startTime=startTime, currentTime=currentTime)

    atlas_bulk_data = get_atlas_data(index=panda_index, query=atlas_query)
    grafana_data = get_grafana_data(url=grafana_url, query=grafana_query)
    grafana_processed_dict = process_grafana_data(grafana_data=grafana_data)

    actions = prepare_es_actions(
        atlas_data=atlas_bulk_data, grafana_dict=grafana_processed_dict)

    try:
        bulk(es, actions)
        print("Data successfully indexed in the destination index.")
    except BulkIndexError as e:
        print("Failed to index documents:")
        for err in e.errors[:3]:
            print(err)


if __name__ == "__main__":
    main()
