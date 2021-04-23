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
