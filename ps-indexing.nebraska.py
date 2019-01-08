# # Checks if Perfsonar data is indexed
#
# Checks number of indexed documents in all perfsonar indices and alerts if any of them is significantly less then usual. It sends mails to all the people substribed to that alert. It is run every 30 min from a cron job.

import matplotlib
matplotlib.use("agg")
import matplotlib.pyplot as plt

matplotlib.rc('font', **{'size': 12})

import pandas as pd

from subscribers import subscribers
import alerts

from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, exceptions as es_exceptions
# es = Elasticsearch(hosts=['gracc.opensciencegrid.org/q/'], scheme='https', port=443, timeout=60)
es = Elasticsearch(['https://gracc.opensciencegrid.org/q/'])

# ### define what are the indices to look for
# first number is interval to check (in hours), second is number in 2 previous intervals, third is number in current interval.

ps_indices = {
    'ps_meta': [24, 0, 0],
    'ps_owd': [1, 0, 0],
    'ps_packet_loss': [1, 0, 0],
    'ps_retransmits': [1, 0, 0],
    'ps_status': [1, 0, 0],
    'ps_throughput': [1, 0, 0],
    'ps_trace': [1, 0, 0]
}

# There is a time offset here - we do now-9 instead of expected now-1.
# two reasons: 1. we get data with a delay 2. there is an issue with timezones even data is indexed in UTC.

sub_end = (datetime.utcnow() - timedelta(hours=9)).replace(microsecond=0, second=0, minute=0)
print('end of subject period: ', sub_end)

for ind in ps_indices:
    print("Checking: ", ind)
    tbin = ps_indices[ind][0]

    ref_start = sub_end - timedelta(hours=tbin * 3)
    ref_end = sub_end - timedelta(hours=tbin)
    print('reference interval:', ref_start, ' till ', ref_end)

    ref_start = int(ref_start.timestamp() * 1000)
    ref_end = int(ref_end.timestamp() * 1000)

    types_query = {
        "size": 0,
        "query": {
            "bool": {
                "filter": {
                    "range": {"timestamp": {"gt": ref_start, 'lte': ref_end}}
                }
            }
        }
    }

    res = es.search(index=ind, body=types_query, request_timeout=120)
    ps_indices[ind][1] = res['hits']['total']

    types_query = {
        "size": 0,
        "query": {
            "bool": {
                "filter": {
                    "range": {"timestamp": {"gt": ref_end, 'lte': int(sub_end.timestamp() * 1000)}}
                }
            }
        }
    }

    res = es.search(index=ind, body=types_query, request_timeout=120)
    ps_indices[ind][2] = res['hits']['total']


df = pd.DataFrame(ps_indices)
df = df[1:].transpose()
df.columns = ["referent", "current"]
df.referent = df.referent / 2
df.plot(kind="bar")
fig = matplotlib.pyplot.gcf()
fig.set_size_inches(6, 6)
plt.tight_layout()
plt.savefig('Images/Check_perfsonar_indexing.Nebraska.png')


df['change'] = df['current'] / df['referent']
df['pr1'] = df['current'] < 10
df['pr2'] = df['change'] < 0.7
df['problem'] = df['pr1'] | df['pr1']
df.head(10)


problematic = df[df['problem'] == True]
print(problematic.head(10))

if problematic.shape[0] > 0:
    S = subscribers()
    A = alerts.alerts()

    test_name = 'Alert on Elastic indexing rate [PerfSonar]'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you know that there is an issue in indexing Perfsonar data in Nebraska Elasticsearch.\n'
        A.send_GUN_HTML_mail(
            'Networking alert',
            user.email,
            body,
            subtitle=test_name,
            images=[
                {
                    "Title": 'Current vs Referent time',
                    "Description": "This plot shows number of documents indexed in two intervals. The Current interval is 1h long except for meta data (24h). Referent interval is just before current interval but is twice longer.",
                    "Filename": "Images/Check_perfsonar_indexing.Nebraska.png",
                    "Link": "http://atlas-kibana.mwt2.org:5601/goto/ac56c27fd9b063b12ee522501f753427"
                }
            ]
        )

        print(user.to_string())
        A.addAlert(test_name, user.name, 'just an issue.')
