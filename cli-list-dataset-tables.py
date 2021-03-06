#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
List dataset's table(s)
Reference: https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.tables.html#list
'''

import httplib2

from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.errors import HttpError
from config import Credential as gc

dataset_id = 'DATASET-ID-HERE'

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
    tables = service.tables()
    response = tables.list(projectId=gc.project_id,
    					   datasetId=dataset_id).execute()

    for table in response['tables']:
        print("%s" % table['id'])

except HttpError as err:
    print 'Error:', err.content
