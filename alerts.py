import os
import re
import time
import datetime as dt
import requests
from dotenv import load_dotenv
import email_report


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
        try:
            res = requests.post(env['AAAS'] + '/alarm/fetch', json=js)
        except requests.RequestException as err:
            print('problem in receiving alarms!', err)
            return []
        if (res.status_code == 200):
            data = res.json()
            print('recieved {} alarms'.format(len(data)))
            return data
        print('problem in receiving alarms!')
        return []

    def getEntry(self, data):
        """What an email shows of the alarm `data`: its source filled into the template."""
        return email_report.alarm_entry(data, self.template)


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
        self.alerts = []  # the alarms to email, see email_report.alarm_entry

    def __str__(self):
        out = f'{self.user} {self.email} preferences: {self.preferences}\nsubs:\n'
        for s in self.subscriptions:
            out += ('\t'+str(s)+'\n')
        return out

    def addAlert(self, entry):
        self.alerts.append(entry)

    def sendMail(self, hours, until=None):
        """Emails the collected alarms of the `hours` before `until` (seconds, by default now),
        as HTML and as plain text."""
        if not self.alerts:
            return
        text = email_report.render_text(self.user, self.alerts, hours, env['AAAS'], until)
        print(text)
        requests.post(
            "https://api.mailgun.net/v3/mg.analytics.mwt2.org/messages",
            auth=("api", env["MAILGUN_API_KEY"]),
            data={
                "from": "ATLAS Alarm & Alert System <aaas@analytics.mwt2.org>",
                "to": [self.preferences['prefered_mail']],
                "subject": email_report.subject(self.alerts),
                "text": text,
                "html": email_report.render_html(self.user, self.alerts, hours, env['AAAS'], until)}
        )


def hours_since_last_mail(hour, interval):
    """How many hours back the previous mail went out to a user mailed at the hours divisible
    by `interval`: e.g. 4 at midnight for every 10 hours (the last went out at 20:00).
    Each mail covers the alarms since the previous one."""
    for back in range(1, 24):
        if (hour - back) % 24 % interval == 0:
            return back
    return 24


def tag_matched(pattern, tags):
    """True if the subscription tag `pattern` is one of the alarm's `tags`, ignoring
    case. '*' in a tag stands for any characters, e.g. *MWT2* matches MWT2_UC."""
    if isinstance(tags, str):
        tags = [tags]
    regex = '.*'.join(re.escape(part) for part in pattern.split('*'))
    return any(re.fullmatch(regex, str(t), re.IGNORECASE) for t in tags)


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
    # each mail covers whole hours up to the start of this one: /alarm/fetch returns alarms from
    # the start of an hour, so alarms of the first minutes of this hour would otherwise be
    # mailed now and again with the next mail
    hourStart = int(time.time() // 3600 * 3600)
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
        hours = hours_since_last_mail(currentHour, mi)

        for s in u.subscriptions:
            print(s)
            try:
                a = alarms(s['category'], s['subcategory'], s['event'])
            except ValueError:
                print('missing:', s['category'], s['subcategory'], s['event'])
                continue
            als = a.getAlarms(hours)
            for al in als:
                if al['created_at'] >= hourStart * 1000:
                    continue  # goes out with the next mail
                if 'tags' in s and 'tags' in al and s['tags'] != '*':
                    tagList = s['tags'].split(' ')
                    found = False
                    for tl in tagList:
                        if tag_matched(tl, al['tags']):
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
                u.addAlert(a.getEntry(al))
        u.sendMail(hours, hourStart)
        print('-------------------------------------------------------------')
