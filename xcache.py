# Checks number of concurrent connections from XCaches to MWT2 dCache.
# Creates alarm if more than 200 from any server.
# ====
# It is run every 30 min from a cron job.

import json
from datetime import datetime
import requests

from alerts import alarms

config_path = '/config/config.json'

with open(config_path) as json_data:
    config = json.load(json_data,)

print('current time', datetime.now())

res = requests.get(
    'http://graphite.mwt2.org/render?target=dcache.xrootd.*&format=json&from=now-2min')
if (res.status_code == 200):
    data = res.json()
    print(data)
    print('recieved data on {} servers'.format(len(data)))
else:
    print('problem in receiving connections!')


ALARM = alarms('Virtual Placement', 'XCache', 'large number of connections')

for server in data:
    serverIP = server['target'].replace('dcache.xrootd.', '').replace('_', '.')
    connections = server['datapoints'][-1][0]
    timestamp = server['datapoints'][-1][1]
    timestamp = datetime.fromtimestamp(timestamp)
    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    if connections < 200:
        print('server {} has {} connections.'.format(serverIP, connections))
    else:
        source = {
            "xcache": serverIP,
            "n_connections": connections,
            "timestamp": timestamp}
        print(source)
        ALARM.addAlarm(
            body='too many connections.',
            tags=[serverIP],
            source=source
        )
