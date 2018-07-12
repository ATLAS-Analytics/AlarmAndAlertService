from subscribers import subscribers
import alerts

S = subscribers()
A = alerts.alerts()

test_name = 'TEST emails'
body = 'Dear Ilija Vukotic,\n\n'
body += '\tthis mail is to let you know that there is an issue in test.\n'

# A.send_HTML_mail(
#     'Test alert',
#     'ilijav@gmail.com',
#     body,
#     subtitle=test_name,
#     images=[
#     ]
# )

A.sendGunMail('Test alert', 'ilijav@gmail.com', body)

A.send_GUN_HTML_mail('Test alert', 'ilijav@gmail.com', body,
                     subtitle=test_name,
                     images=[
                         {
                             "Title": 'Current vs Referent time',
                             "Description": "This plot shows number of documents indexed in two intervals. The Current interval is 1h long except for meta data (24h). Referent interval is just before current interval but is twice longer.",
                             "Filename": "Images/R.jpg",
                             "Link": "http://atlas-kibana.mwt2.org:5601/goto/ac56c27fd9b063b12ee522501f753427"
                         }
                     ])
