import os
import time
import datetime as dt
import requests
from dotenv import load_dotenv


load_dotenv()
env = {}
for var in ['AAAS', 'MAILGUN_API_KEY']:
    env[var] = os.environ.get(var, None)
    if not env[var]:
        print('environment variable {} not set!'.format(var))
        raise ValueError('environment variable {} not set!'.format(var))

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
            raise ValueError('This category does not exist any more!')

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
        res = requests.post(env['AAAS'] + '/alarm', json=js)
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
        res = requests.post(env['AAAS'] + '/alarm/fetch', json=js)
        if (res.status_code == 200):
            data = res.json()
            print('recieved {} alarms'.format(len(data)))
            return data
        print('problem in receiving alarms!')

    def getText(self, data):
        res = ''
        res += time.strftime("%a, %d %b %Y %H:%M:%S",
                             time.localtime(data['created_at']/1000))
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
                if 'c{'+v+'}' in temp:
                    temp = temp.replace('c{'+v+'}', str(len(vars[v])))
                if 'p{'+v+'}' in temp:
                    temp = temp.replace(
                        'p{'+v+'}', str(round(vars[v]*100, 3))+'%')
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
        # site issues the user muted on the subscriptions page
        self.acknowledgements = data['_source'].get('acknowledgements', [])
        if 'prefered_mail' not in self.preferences or self.preferences['prefered_mail'] == '':
            self.preferences['prefered_mail'] = self.email
        self.mail = ''

    def __str__(self):
        out = f'{self.user} {self.email} preferences: {self.preferences}\nsubs:\n'
        for s in self.subscriptions:
            out += ('\t'+str(s)+'\n')
        return out

    def addAlert(self, body):
        self.mail += body+'\n\n'

    def addHeaderFooter(self):
        header = f'Dear {self.user},\n\n\t'
        header += 'Herewith a list of alarms you subscribed to. '
        header += f'Preferences may be changed by {self.email} by visiting {env["AAAS"]}.\n'
        footer = 'Best regards,\n Alarm & Alert Service Team'
        self.mail = header+self.mail+footer

    def sendMail(self):
        if not self.mail:
            return
        self.addHeaderFooter()
        print(self.mail)
        requests.post(
            "https://api.mailgun.net/v3/mg.analytics.mwt2.org/messages",
            auth=("api", env["MAILGUN_API_KEY"]),
            data={
                "from": "ATLAS Alarm & Alert System <aaas@analytics.mwt2.org>",
                "to": [self.preferences['prefered_mail']],
                "subject": 'Alarm & Alert System delivery',
                "text": self.mail}
        )


def level_ok(sub, alarm):
    """True if the subscription wants this alarm's severity level.
    A missing or '*' `levels` means all levels; alarms without a level are always sent."""
    levels = sub.get('levels', '*')
    if levels == '*' or 'level' not in alarm:
        return True
    return str(alarm['level']) in levels.split(' ')


def is_acknowledged(user, sub, alarm, now_ms):
    """True if the user muted (acknowledged) one of this alarm's tags for this event.
    A mute limited to some severity `levels` lets alarms of other levels through."""
    level = alarm.get('level')
    muted = {a['tag'] for a in user.acknowledgements
             if a['until'] > now_ms and (a['category'], a['subcategory'], a['event']) ==
             (sub['category'], sub['subcategory'], sub['event'])
             and (not a.get('levels') or level is None or str(level) in a['levels'])}
    tags = alarm.get('tags', [])
    if isinstance(tags, str):
        tags = [tags]
    return bool(muted) and any(t in muted for t in tags)


def getCategories():
    global categories
    res = requests.get(env['AAAS'] + '/alarm/categories')
    if (res.status_code == 200):
        categories = res.json()
        # print(categories)
        print('recieved {} categories'.format(len(categories)))
    else:
        print('problem in receiving categories!')


def getUsers():
    res = requests.get(env['AAAS'] + '/user')
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
    currentHour = dt.datetime.now().hour
    print('current hour:', currentHour)
    for u in users:

        print(u.user, u.preferences)

        # setting defaults if not there
        if 'vacation' not in u.preferences:
            u.preferences['vacation'] = False
        if 'mail_interval' not in u.preferences:
            u.preferences['mail_interval'] = 6
        if u.preferences['vacation']:  # skip in on vacation
            continue

        # skip if not appropriate hour.
        mi = int(u.preferences['mail_interval'])
        if mi == 0:
            mi = 1
        if currentHour % mi:
            print('not yet...')
            continue

        for s in u.subscriptions:
            print(s)
            try:
                a = alarms(s['category'], s['subcategory'], s['event'])
            except ValueError:
                print('missing:', s['category'], s['subcategory'], s['event'])
                continue
            als = a.getAlarms(mi)
            for al in als:
                if 'tags' in s and 'tags' in al and s['tags'] != '*':
                    tagList = s['tags'].split(' ')
                    found = False
                    for tl in tagList:
                        if tl in al['tags']:
                            found = True
                    if not found:
                        print('tag not matched:', al['tags'])
                        continue
                    print('tag matched.')
                if not level_ok(s, al):
                    print('level not matched:', al['level'])
                    continue
                if is_acknowledged(u, s, al, time.time() * 1000):
                    print('acknowledged, skipping:', al['tags'])
                    continue
                alert_text = a.getText(al)
                u.addAlert(alert_text)
        u.sendMail()
        print('-------------------------------------------------------------')
