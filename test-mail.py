from subscribers import subscribers
import alerts

S = subscribers()
A = alerts.alerts()

test_name = 'TEST emails'
body = 'Dear Ilija Vukotic,\n\n'
body += '\tthis mail is to let you know that there is an issue in test.\n'
A.send_HTML_mail(
    'Test alert',
    'ilijav@gmail.com',
    body,
    subtitle=test_name,
    images=[
    ]
)
