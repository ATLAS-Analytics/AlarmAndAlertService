import requests
import json
host = 'https://aaas.atlas-ml.org/alarm/'


class alarms:

    def addAlarm(self, category, subcategory, event, body, tags=[]):
        js = {
            "category": category,
            "subcategory": subcategory,
            "event": event,
            "body": body,
            "tags": tags
        }
        res = requests.post(host, json=js)
        print(res)
        print(res.status_code)
