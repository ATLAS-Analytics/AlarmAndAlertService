import os
import requests
import httplib2
import json
import time
from oauth2client.service_account import ServiceAccountCredentials
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

from subprocess import Popen, PIPE


class alerts:

    def __init__(self):
        SCOPE = ["https://spreadsheets.google.com/feeds"]
        SECRETS_FILE = "/secrets/AlertingService-879d85ad058f.json"
        credentials = ServiceAccountCredentials.from_json_keyfile_name(SECRETS_FILE, SCOPE)
        http = credentials.authorize(httplib2.Http())
        discoveryUrl = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
        self.service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)
        return

    def addAlert(self, test, email, text):
        spreadsheetId = '19bS4cxqBEwr_cnCEfAkbaLo9nCLjWnTnhQHsZGK9TYU'
        rangeName = test + '!A1:C1'
        myBody = {u'range': rangeName, u'values': [[time.strftime("%Y/%m/%d %H:%M:%S"), email, text]], u'majorDimension': u'ROWS'}
        cells = self.service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=rangeName,
                                                            valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=myBody).execute()
        return

    def sendMail(self, test, to, body):
        msg = MIMEText(body)
        msg['Subject'] = test
        msg['From'] = 'AAAS@mwt2.org'
        msg['To'] = to

        p = Popen(["/usr/sbin/sendmail", "-t", "-oi", "-r AAAS@mwt2.org"], stdin=PIPE)
        print(msg.as_string())
        p.communicate(msg.as_string().encode('utf-8'))

    def sendGunMail(self, test, to, body):
        requests.post(
            "https://api.mailgun.net/v3/analytics.mwt2.org/messages",
            auth=("api", os.environ['MAILGUN_API_KEY']),
            data={
                "from": "ATLAS Alarm & Alert System <aaas@analytics.mwt2.org>",
                "to": [to],
                "subject": test,
                "text": body}
        )

    def send_HTML_mail(self, test, to, body, subtitle="", images=[]):
        msg = MIMEMultipart('related')
        msg['Subject'] = test
        msg['From'] = 'AAAS@mwt2.org'
        msg['To'] = to

        msgAlternative = MIMEMultipart('alternative')
        msg.attach(msgAlternative)

        html = open("index.htm", "r").read()
        image_template = open("image_template.htm", "r").read()

        html = html.replace('TheMainTitle', test)
        html = html.replace('TheSubtitle', subtitle)
        html = html.replace('MyBody', body)

        html = html.replace('TheImagePlaceholder1', image_template * int((len(images) + 1) / 2))
        html = html.replace('TheImagePlaceholder2', image_template * int(len(images) / 2))

        for ind, i in enumerate(images):
            #print("Adding image:", i)
            html = html.replace('FigureTitle', i['Title'], 2)  # appears twice per figure
            html = html.replace('FigureFilename', "cid:image" + str(ind), 1)
            html = html.replace('FigureDescription', i['Description'], 1)
            link = ''
            if 'Link' in i:
                link = i['Link']
            html = html.replace('FigureLink', link, 1)

            img_data = open(i['Filename'], 'rb').read()
            image = MIMEImage(img_data, name=i['Filename'])
            image.add_header('Content-ID', '<image' + str(ind) + '>')
            msg.attach(image)

        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(body, 'plain')
        part2 = MIMEText(html, 'html')

        msgAlternative.attach(part1)
        msgAlternative.attach(part2)

        p = Popen(["/usr/sbin/sendmail", "-t", "-oi", "-r AAAS@mwt2.org"], stdin=PIPE)
        # print(msg.as_string())
        p.communicate(msg.as_string().encode('utf-8'))

    def send_GUN_HTML_mail(self, test, to, body, subtitle="", images=[]):

        html = open("index.htm", "r").read()
        image_template = open("image_template.htm", "r").read()

        html = html.replace('TheMainTitle', test)
        html = html.replace('TheSubtitle', subtitle)
        html = html.replace('MyBody', body)

        html = html.replace('TheImagePlaceholder1', image_template * int((len(images) + 1) / 2))
        html = html.replace('TheImagePlaceholder2', image_template * int(len(images) / 2))

        ims = []
        for ind, i in enumerate(images):
            #print("Adding image:", i)
            html = html.replace('FigureTitle', i['Title'], 2)  # appears twice per figure
            html = html.replace('FigureFilename', "cid:image" + str(ind), 1)
            html = html.replace('FigureDescription', i['Description'], 1)
            link = ''
            if 'Link' in i:
                link = i['Link']
            html = html.replace('FigureLink', link, 1)
            ims.append(("inline", ("image" + str(ind), open(i['Filename'], 'rb').read())))

        requests.post(
            "https://api.mailgun.net/v3/analytics.mwt2.org/messages",
            auth=("api", os.environ['MAILGUN_API_KEY']),
            files=ims,
            data={
                "from": "ATLAS Alarm & Alert System <aaas@analytics.mwt2.org>",
                "to": [to],
                "subject": test,
                "text": body,
                "html": html
            }
        )
