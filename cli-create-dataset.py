#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Create dataset
Reference: https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.datasets.html#insert
'''

import httplib2

from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.errors import HttpError
from config import Credential as gc

new_dataset_id = 'DATASET-NAME-HERE'

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
    request_body = {'datasetReference': {'datasetId': new_dataset_id}}

    response = datasets.insert(projectId=gc.project_id,
                               body=request_body).execute()

    # print out the response
    print(response);

except HttpError as err:
    print 'Error:', err.content
