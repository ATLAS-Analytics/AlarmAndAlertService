# Checks Squid failovers from US sites.
# Creates alarm if any are found.
# ====
# It is run every hour from a cron job.

from datetime import datetime
import json
import requests

from alerts import alarms

config_path = '/config/config.json'

with open(config_path) as json_data:
    config = json.load(json_data,)

print('current time', datetime.now())
current_hour = datetime.now().hour

sites = ['MWT2', 'AGLT2', 'SWT2_CPB', 'BU_ATLAS_Tier2']
failovers = {a: {'servers': 0, 'requests': 0, 'data': 0} for a in sites}

res = requests.get(
    'http://wlcg-squid-monitor.cern.ch/failover/failoverATLAS/failover-record-nosquid.tsv')
if (res.status_code == 200):
    data = res.content.decode("utf-8")
    lines = data.splitlines()
    # ['Timestamp', 'Group', 'Sites', 'Host', 'Ip', 'Bandwidth', 'BandwidthRate', 'Hits', 'HitsRate']
    for line in lines[1:]:
        vals = line.split('\t')
        if datetime.fromtimestamp(int(vals[0])).hour != current_hour:
            continue
        site = vals[2]
        if site in sites:
            failovers[site]['servers'] += 1
            failovers[site]['requests'] += int(vals[7])
            failovers[site]['data'] += int(vals[5])

    print('failovers:', failovers)
else:
    print('problem in receiving connections!')


ALARM = alarms('SLATE', 'Squid', 'failovers')

for site, details in failovers.items():
    if details['servers'] == 0:
        continue
    source = {
        "site": site,
        "WNs": details['servers'],
        "requests": details['requests'],
        "data": details['data']
    }
    print(source)
    ALARM.addAlarm(
        body='failover',
        tags=[site],
        source=source
    )

print('Done.')
