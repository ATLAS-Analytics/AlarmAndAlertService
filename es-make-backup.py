# makes a snapshot

import sys
import requests

from datetime import date
today = date.today()
print("Today's date:", today)

import json
with open('/config/config.json') as json_data:
    config = json.load(json_data,)

ES_CONN = 'http://' + config['ES_USER'] + ':' + config['ES_PASS'] + '@' + config['ES_HOST'] + ':9200'
command = ES_CONN + '/_snapshot/my_backup/auto-' + today.strftime("%Y-%m-%d")
print("command:", command)
r = requests.put(command)
res = r.json()
print(res)
