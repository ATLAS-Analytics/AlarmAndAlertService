# Check Cluster State
# ====
# This notebook check the state of ES cluster and sends mails to all the people substribed to that alert.
# It is run once per hour from a cron job.

import sys
import subscribers
import alerts
import requests

import json
with open('config.json') as json_data:
    config = json.load(json_data,)

ES_CONN = 'http://' + config['ES_USER'] + ':' + config['ES_PASS'] + '@' + config['ES_HOST'] + ':9200/_cluster/health'
r = requests.get(ES_CONN)
res = r.json()
print(res)


if res['status'] == 'green':
    sys.exit(0)

S = subscribers.subscribers()
A = alerts.alerts()

if res['status'] == 'red':
    testName = 'Alert on Elastic cluster state [ES in red]'
    subscribersToRed = S.get_immediate_subscribers(testName)
    for subscriber in subscribersToRed:
        body = 'Dear ' + subscriber.name + ',\n\n'
        body = body + '\tthis mail is to let you that the University of Chicago Elasticsearch cluster is in RED.\n'
        body = body + '\nBest regards,\nATLAS AAS'
        body = body + '\n\n To change your alerts preferences please you the following link:\n' + subscriber.link
        print(subscriber.to_string())
        A.sendGunMail(testName, subscriber.email, body)
        A.addAlert(testName, subscriber.name, 'simply red.')
if res['status'] == 'yellow' and res['unassigned_shards'] > 10:
    testName = 'Alert on Elastic cluster state [ES in yellow]'
    subscribersToYellow = S.get_immediate_subscribers(testName)
    for subscriber in subscribersToYellow:
        body = 'Dear ' + subscriber.name + ',\n\n'
        body = body + '\tthis mail is to let you that the University of Chicago Elasticsearch cluster is in YELLOW.'
        body = body + 'There is ' + str(res['unassigned_shards']) + ' unassigned shards on ' + str(res['number_of_nodes']) + ' nodes.\n'
        body = body + '\nBest regards,\nATLAS AAS'
        body = body + '\n\n To change your alerts preferences please you the following link:\n' + subscriber.link
        print(subscriber.to_string())
        A.sendGunMail(testName, subscriber.email, body)
        A.addAlert(testName, subscriber.name, str(res['unassigned_shards']) +
                   ' unassigned shards on ' + str(res['number_of_nodes']) + ' nodes.')
