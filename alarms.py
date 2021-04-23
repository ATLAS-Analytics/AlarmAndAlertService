import requests
import json

config_path = '/config/config.json'
config_path = 'kube/secrets/config.json'

with open(config_path) as json_data:
    config = json.load(json_data,)


class alarms:
    def __init__(self, category, subcategory, event):
        self.category = category
        self.subcategory = subcategory
        self.event = event

    def addAlarm(self, body, tags=[]):
        js = {
            "category": self.category,
            "subcategory": self.subcategory,
            "event": self.event,
            "body": body,
            "tags": tags
        }
        res = requests.post(config['AAAS'] + '/alarm', json=js)
        if (res.status_code == 200):
            print('created alarm: {}:{}:{} {} {}'.format(
                self.category, self.subcategory, self.event, body, tags))
        else:
            print('problem in creating alarm!')
