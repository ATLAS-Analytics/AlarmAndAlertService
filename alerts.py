import os
import time
import requests
import json

config_path = '/config/config.json'
# config_path = 'kube/secrets/config.json'
mailgun_api_key = os.environ['MAILGUN_API_KEY']

with open(config_path) as json_data:
    config = json.load(json_data,)

categories = []


class alarms:
    def __init__(self, category, subcategory, event):
        self.category = category
        self.subcategory = subcategory
        self.event = event
        self.template = ''
        self.check_it()

    def check_it(self):
        if not categories:
            return
        found = False
        for c in categories:
            if (c['category'] == self.category and
                    c['subcategory'] == self.subcategory and c['event'] == self.event):
                found = True
                if 'template' in c:
                    self.template = c['template']
        if not found:
            print('ERROR This category does not exist any more!')

    def addAlarm(self, body, tags=[], source=None, level=None):
        js = {
            "category": self.category,
            "subcategory": self.subcategory,
            "event": self.event,
            "body": body,
            "tags": tags
        }
        if source:
            js['source'] = source
        if level:
            js['level'] = level
        res = requests.post(config['AAAS'] + '/alarm', json=js)
        if (res.status_code == 200):
            print('created alarm: {}:{}:{} {} {}'.format(
                self.category, self.subcategory, self.event, body, tags))
        else:
            print('problem in creating alarm!')
            print(res.text, res.status_code)

    def getAlarms(self, period):
        js = {
            "category": self.category,
            "subcategory": self.subcategory,
            "event": self.event,
            "period": period
        }
        res = requests.post(config['AAAS'] + '/alarm/fetch', json=js)
        if (res.status_code == 200):
            data = res.json()
            print('recieved {} alarms'.format(len(data)))
            return data
        print('problem in receiving alarms!')

    def getText(self, data):
        res = ''
        res += time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(data['created_at']/1000))
        res += '\t{}/{}/{}\t\t{}\n'.format(data['category'],
                                           data['subcategory'], data['event'], data['body'])
        if 'tags' in data:
            res += 'tags: {}\n'.format(', '.join(data['tags']))
        if 'level' in data:
            res += 'Level: {}\n'.format(data['level'])
        if self.template and 'source' in data:
            vars = data['source']
            temp = self.template
            for v in vars:
                if '%{'+v+'}' in temp:
                    temp = temp.replace('%{'+v+'}', str(vars[v]))
            res += temp
        # print(data)
        return res


class user:
    def __init__(self, data):
        self.id = data['_id']
        self.username = data['_source']['username']
        self.user = data['_source']['user']
        self.email = data['_source']['email']
        self.subscriptions = data['_source']['subscriptions']
        self.preferences = data['_source']['preferences']
        self.mail = ''

    def __str__(self):
        out = '{} {} preferences: {}\nsubs:\n'.format(self.user, self.email, self.preferences)
        for s in self.subscriptions:
            out += ('\t'+str(s)+'\n')
        return out

    def addAlert(self, body):
        self.mail += body+'\n\n'

    def addHeaderFooter(self):
        header = 'Dear '+self.user+',\n\n\t'
        header += 'Herewith a list of alarms you subscribed to. '
        header += 'You may change preferences by visiting '+config['AAAS']+'.\n'
        footer = 'Best regards,\n Alarm & Alert Service Team'
        self.mail = header+self.mail+footer

    def sendMail(self):
        if not self.mail:
            return
        self.addHeaderFooter()
        print(self.mail)
        requests.post(
            "https://api.mailgun.net/v3/analytics.mwt2.org/messages",
            auth=("api", mailgun_api_key),
            data={
                "from": "ATLAS Alarm & Alert System <aaas@analytics.mwt2.org>",
                "to": [self.email],
                "subject": 'Alarm & Alert System delivery',
                "text": self.mail}
        )


def getCategories():
    global categories
    res = requests.get(config['AAAS'] + '/alarm/categories')
    if (res.status_code == 200):
        categories = res.json()
        # print(categories)
        print('recieved {} categories'.format(len(categories)))
    else:
        print('problem in receiving categories!')


def getUsers():
    res = requests.get(config['AAAS'] + '/user')
    users = []
    if (res.status_code == 200):
        data = res.json()
        for udata in data:
            u = user(udata)
            print(u)
            users.append(u)
        print('recieved {} users'.format(len(users)))
        return users
    print('problem in receiving users!')


if __name__ == '__main__':
    getCategories()
    users = getUsers()
    for u in users:
        if u.preferences['vacation']:
            continue

        for s in u.subscriptions:
            a = alarms(s['category'], s['subcategory'], s['event'])
            als = a.getAlarms(24)
            for al in als:
                alert_text = a.getText(al)
                u.addAlert(alert_text)

        u.sendMail()
