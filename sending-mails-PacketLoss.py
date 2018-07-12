# # Send alert emails about packet loss based on alarms and user subscribing

# This notebook is run by a cron job every hour, and its purpose is to send alert emails about packet loss for user specified site(s) based on alarms and user subscribing records.
#
# This notebook works following this procedure:
# (1) Get all the alarms of type packetloss for the past hour (call it NEW) and past past hour (call it OLD) from Elasticsearch
# (2) Get the user subscribing records from Google Sheets calling APIs in subscribers.py
# (3) Process the alarms data and subscribing data to make them easier to use for this monitoring task
# (4) TN_old means total number of alarmed links involving a specific site ip (no matter from it or to it) for OLD time period
# (5) TN_new means total number of alarmed links involving a specific site ip (no matter from it or to it) for NEW time period
# (6) TN_delta means the change of value from TN_old to TN_new. We need to compare TN_delta v.s. +N and v.s. -N (tune N later)
# (7) If a site ip never occurs in NEW and OLD, then it must be totally fine, and we do not care about it at all (TN_old == TN_new == TN_delta == 0)
# (8) If a site ip occurs in NEW or OLD or both, then we may have TN_delta > 0 or == 0 or < 0 for this site ip, so we want to take a closer look at this site ip, so we do (9) (10) (11)
# (9) If TN_delta >= +N, then overall the links connected to this site are becoming worse, so we send email
# (10) If TN_delta <= -N, then overall the links connected to this site are becoming better, so we send email
# (11) Otherwise, the overall status for this site is not changing or just changing slightly, so we do not send email
# (12) In order to send email, we need a dictionary whose key is site ip and value is a list of relevant user emails

# Regular Expression
import re

# Retrieve user subscribing records from google sheets.
from subscribers import subscribers
import alerts

S = subscribers()
A = alerts.alerts()

# Related to Elasticsearch queries
from elasticsearch import Elasticsearch, exceptions as es_exceptions, helpers


# ## Establish Elasticsearch connection
es = Elasticsearch(hosts=[{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)


# ## Queries to find all the alarms of type Packet Loss for the past hour and past past hour

query_new = {
    "size": 1000,
    "query": {
        "bool": {
            "must": [
                {"term": {"type": "packetloss"}}
            ],
            "filter": {
                "range": {
                    "alarmTime": {
                        "gt": "now-3h"
                    }
                }
            }
        }
    }
}

query_old = {
    "size": 1000,
    "query": {
        "bool": {
            "must": [
                {"term": {"type": "packetloss"}}
            ],
            "filter": {
                "range": {
                    "alarmTime": {
                        "gt": "now-6h",
                        "lt": "now-3h"
                    }
                }
            }
        }
    }
}

print(query_new)
print(query_old)


# ## Execute the query
result_new = es.search(index='alarms', body=query_new, request_timeout=120)
print('Number of hits of new alarms:', result_new['hits']['total'])

result_old = es.search(index='alarms', body=query_old, request_timeout=120)
print('Number of hits of old alarms:', result_old['hits']['total'])

hits_new = result_new['hits']['hits']
hits_old = result_old['hits']['hits']


# ## Generate the two dictionaries for sites, one is from ip to name, one is from name to ip

site_ip_name = {}

for hit in hits_new:
    info = hit['_source']
    site_ip_name[info['src']] = info['srcSite']
    site_ip_name[info['dest']] = info['destSite']

for hit in hits_old:
    info = hit['_source']
    site_ip_name[info['src']] = info['srcSite']
    site_ip_name[info['dest']] = info['destSite']

print(site_ip_name)

site_name_ip = {}

for ip in site_ip_name:
    name = site_ip_name[ip]
    if name in site_name_ip:
        site_name_ip[name].append(ip)
    else:
        site_name_ip[name] = [ip]

print(site_name_ip)


# ## Calculate TN_old, the total number of alarmed links involving a specific site ip (either as source site or as destination site) for the OLD time period


TN_old = {}


def TN_old_add_one(ip):
    if ip in TN_old:
        TN_old[ip] += 1
    else:
        TN_old[ip] = 1


for alarm in hits_old:
    TN_old_add_one(alarm['_source']['src'])
    TN_old_add_one(alarm['_source']['dest'])

# TN_old

# ## Calculate TN_new, the total number of alarmed links involving a specific site ip (either as source site or as destination site) for the NEW time period

TN_new = {}


def TN_new_add_one(ip):
    if ip in TN_new:
        TN_new[ip] += 1
    else:
        TN_new[ip] = 1


for alarm in hits_new:
    TN_new_add_one(alarm['_source']['src'])
    TN_new_add_one(alarm['_source']['dest'])

# TN_new


# ## Calculate TN_delta, which is equal to ( TN_new - TN_old )

TN_delta = {}

for ip in TN_old:
    if ip in TN_new:
        TN_delta[ip] = TN_new[ip] - TN_old[ip]
    else:
        TN_delta[ip] = -TN_old[ip]

for ip in TN_new:
    if ip not in TN_old:
        TN_delta[ip] = TN_new[ip]

TN_delta


# ## Look at the distribution of TN_delta, so that we can tune the parameter N

for N in range(10):
    count_worse = 0
    count_better = 0
    count_stable = 0
    for ip in TN_delta:
        if TN_delta[ip] > N:
            count_worse += 1
        elif TN_delta[ip] < -N:
            count_better += 1
        else:
            count_stable += 1
    print('N=%d     links went bad=%d     links went good=%d     unchanged=%d' % (N, count_worse, count_better, count_stable))


# ## Let's use N=6 for now, and we will tune later

N = 6

ip_list_worse = []
ip_list_better = []

for ip in TN_delta:
    if TN_delta[ip] >= N:
        ip_list_worse.append(ip)
    elif TN_delta[ip] <= -N:
        ip_list_better.append(ip)

print('--- The ip of the site(s) which got worse:')
print(ip_list_worse)
print('--- The ip of the site(s) which got better:')
print(ip_list_better)


# ## Generate the dictionary: key = site name, value = a list of relevant user emails

user_interest_site_name = {}


def reg_user_interest_site_name(sitename, email):
    if sitename in user_interest_site_name:
        user_interest_site_name[sitename].append(email)
    else:
        user_interest_site_name[sitename] = [email]


test_name = 'PerfSONAR [Packet loss change for link(s) where your site is a source or destination]'
emailSubject = 'Significant change in the number of network paths with large packet loss where your subscribed site(s) are the source or destination'

users = S.get_immediate_subscribers(test_name)

# Handle blank answer, one site, several sites separated by comma, wildcard such as prefix* etc.
for user in users:
    sitenames = user.sites
    print(user.to_string(), sitenames)
    if len(sitenames) == 0:
        sitenames = ['.']  # Handle blank answer, so match all site names
    sitenames = [x.replace('*', '.') for x in sitenames]  # Handle several site names, and wildcard
    for sn in sitenames:
        p = re.compile(sn, re.IGNORECASE)
        for sitename in site_name_ip:
            if p.match(sitename):
                reg_user_interest_site_name(sitename, user)


# ## Generate the dictionary: key = site ip, value = a list of relevant user emails

user_interest_site_ip = {}


def reg_user_interest_site_ip(siteip, email):
    if siteip in user_interest_site_ip:
        user_interest_site_ip[siteip].append(email)
    else:
        user_interest_site_ip[siteip] = [email]


for sitename in user_interest_site_name:
    for siteip in site_name_ip[sitename]:
        for user in user_interest_site_name[sitename]:
            reg_user_interest_site_ip(siteip, user)

print(user_interest_site_ip)


# ## Generate info for sending alert emails (for the sites getting worse)

for ip in ip_list_worse:
    text = "The site %s (%s)'s network paths have worsened, the count of src-destination paths with packet-loss went from %d to %d.\n" % (
        site_ip_name[ip], ip, TN_old.get(ip, 0), TN_new.get(ip, 0))
    text += "These are all the problematic src-destination paths for the past hour:\n"
    for alarm in hits_new:
        src_ip = alarm['_source']['src']
        dest_ip = alarm['_source']['dest']
        if src_ip == ip:
            text += '    %s (%s)  --->  %s (%s) \n' % (site_ip_name[src_ip], src_ip, site_ip_name[dest_ip], dest_ip)
    for alarm in hits_new:
        src_ip = alarm['_source']['src']
        dest_ip = alarm['_source']['dest']
        if dest_ip == ip:
            text += '    %s (%s)  --->  %s (%s) \n' % (site_ip_name[src_ip], src_ip, site_ip_name[dest_ip], dest_ip)
    print(text)
    if ip not in user_interest_site_ip:
        continue
    for user in user_interest_site_ip[ip]:
        user.alerts.append(text)


# ## Generate info for sending alert emails (for the sites getting better)

for ip in ip_list_better:
    text = "The site %s (%s)'s network paths have improved, the count of src-destination paths with packet-loss went from %d to %d.\n" % (
        site_ip_name[ip], ip, TN_old.get(ip, 0), TN_new.get(ip, 0))
    wtext = ""
    for alarm in hits_new:
        src_ip = alarm['_source']['src']
        dest_ip = alarm['_source']['dest']
        if src_ip == ip:
            text += '    %s (%s)  --->  %s (%s) \n' % (site_ip_name[src_ip], src_ip, site_ip_name[dest_ip], dest_ip)
    for alarm in hits_new:
        src_ip = alarm['_source']['src']
        dest_ip = alarm['_source']['dest']
        if dest_ip == ip:
            text += '    %s (%s)  --->  %s (%s) \n' % (site_ip_name[src_ip], src_ip, site_ip_name[dest_ip], dest_ip)
    if len(wtext) > 0:
        text += "These are the remaining problematic src-destination paths for the past hour:\n"
        text += wtext
#    print(text)
    if ip not in user_interest_site_ip:
        continue
    for user in user_interest_site_ip[ip]:
        user.alerts.append(text)

# user_alert_all

# ## Send out alert email customized for each user


for user in users:
    if len(user.alerts) > 0:
        body = 'Dear ' + user.name + ',\n\n'
        body = body + '\tThis mail is to let you know that there are significant changes in the number of paths with large packet-loss detected by perfSONAR for sites you requested alerting about.\n\n'
        for a in user.alerts:
            body = body + a + '\n'

        # Add in two items: 1) Where to go for more information and 2) who to contact to pursue fixing this   +SPM 20-Apr-2017
        body += '\n To get more information about this alert message and its interpretation, please visit:\n'
        body += '  http://twiki.opensciencegrid.org/bin/view/Documentation/NetworkingInOSG/PacketLossAlert\n'
        body += '\n If you suspect a network problem and wish to follow up on it please email the appropriate support list:\n'
        body += '     For OSG sites:  goc@opensciencegrid.org using Subject: Possible network issue\n'
        body += '     For WLCG sites:  wlcg-network-throughput@cern.ch using Subject: Possible network issue\n'
        body += ' Please include this alert email to help expedite your request for network debugging support.\n'
        body += '\n To change your alerts preferences please use the following link:\n' + user.link
        body += '\n\nBest regards,\nATLAS Networking Alert Service'
        # print(body)
        A.sendGunMail(emailSubject, user.email, body)
        A.addAlert(test_name, user.name, 'change in packet loss')
