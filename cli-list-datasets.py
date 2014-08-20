#!/usr/bin/env python

'''
List project's dataset(s)
Reference: https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.datasets.html#list
'''

import httplib2

from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.errors import HttpError
from config import Credential as gc

f = file(gc.private_key, 'rb')
key = f.read()
f.close()

credentials = SignedJwtAssertionCredentials(
    gc.service_account_email,
    key,
    scope='https://www.googleapis.com/auth/bigquery')

http = httplib2.Http()
http = credentials.authorize(http)

service = build('bigquery', 'v2', http=http)

try:
    datasets = service.datasets()
    response = datasets.list(projectId=gc.project_id).execute()

    for dataset in response['datasets']:
        print("%s" % dataset['id'])

except HttpError as err:
    print 'Error:', err.content
