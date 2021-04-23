# AlarmAndAlertService

For all ATLAS alarm &amp; alert codes.

## Alarm list

* sending mails
* packet loss
* jobs and task indexing
* FTS calculate and index
* Frontier Failed Qs
* Frontier Threads
* User reports

## TO DO

* To all alarms add a field that contains mail template.
* Make alert generation a separate code. Run it in a separate cron job.

    test_name = 'Alert on Elastic indexing rate [Panda Jobs]'
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you that there is an issue in indexing Panda Jobs data in UC Elasticsearch.\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please you the following link:\n' + user.link
        A.sendGunMail(test_name, user.email, body)

    test_name = 'Alert on Elastic indexing rate [Panda Tasks]'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you that there is an issue in indexing Jedi tasks data in UC Elasticsearch.\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please you the following link:\n' + user.link
        A.sendGunMail(test_name, user.email, body)

    test_name = 'Alert on Elastic indexing rate [Panda Task Parameters]'
    users = S.get_immediate_subscribers(test_name)
    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you that there is an issue in indexing Jedi task parameters data in UC Elasticsearch.\n'
        body += '\nBest regards,\nATLAS AAS'
        body += '\n\n To change your alerts preferences please you the following link:\n' + user.link
        A.sendGunMail(test_name, user.email, body)

    for user in users:
        body = 'Dear ' + user.name + ',\n\n'
        body += '\tthis mail is to let you know that there is an issue in indexing Perfsonar data in UC Elasticsearch.\n'
        A.send_GUN_HTML_mail(
            'Networking alert',
            user.email,
            body,
            subtitle=test_name,
            images=[
                {
                    "Title": 'Current vs Referent time',
                    "Description": "This plot shows number of documents indexed in two intervals. The Current interval is 1h long except for meta data (24h). Referent interval is just before current interval but is twice longer.",
                    "Filename": "Images/Check_perfsonar_indexing.png",
                    "Link": "https://atlas-kibana.mwt2.org/s/networking/app/kibana#/visualize/edit/58bf3e80-18d1-11e8-8f2f-ab6704660c79?_g=(refreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A(from%3Anow-7d%2Cmode%3Aquick%2Cto%3Anow))"
                }
            ]
        )

        print(user.to_string())
        A.addAlert(test_name, user.name, 'just an issue.')



    subscribersToRed = S.get_immediate_subscribers(testName)
    for subscriber in subscribersToRed:
        body = 'Dear ' + subscriber.name + ',\n\n'
        body = body + '\tthis mail is to let you that the University of Chicago Elasticsearch cluster is in RED.\n'
        body = body + '\nBest regards,\nATLAS AAS'
        body = body + '\n\n To change your alerts preferences please you the following link:\n' + subscriber.link
        print(subscriber.to_string())
        A.sendGunMail(testName, subscriber.email, body)

    
    subscribersToYellow = S.get_immediate_subscribers(testName)
    msg = str(res['unassigned_shards']) + ' unassigned shards on ' + \
        str(res['number_of_nodes']) + ' nodes.'
    for subscriber in subscribersToYellow:
        body = 'Dear ' + subscriber.name + ',\n\n'
        body = body + '\tthis mail is to let you that the University of Chicago Elasticsearch cluster is in YELLOW.'
        body = body + 'There is ' + \
            str(res['unassigned_shards']) + ' unassigned shards on ' + \
            str(res['number_of_nodes']) + ' nodes.\n'
        body = body + '\nBest regards,\nATLAS AAS'
        body = body + '\n\n To change your alerts preferences please you the following link:\n' + subscriber.link
        print(subscriber.to_string())
        A.sendGunMail(testName, subscriber.email, body)
