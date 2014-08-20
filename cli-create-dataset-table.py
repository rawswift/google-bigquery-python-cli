#!/usr/bin/env python

'''
Create dataset table (w/ sample schema)
Reference: https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.tables.html#insert
'''

import httplib2

from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.errors import HttpError
from config import Credential as gc

dataset_id = 'DATASET-ID-HERE'
new_table_id = 'NEW-TABLE-NAME-HERE'

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

    # Construct the request body object w/ sample table schema
    request_body = {
                    "schema": {
                      "fields": [
                        {
                          "mode": "REQUIRED",
                          "type": "STRING",
                          "name": "id",
                        },
                        {
                          "mode": "REQUIRED",
                          "type": "STRING",
                          "name": "full_name",
                        },
                        {
                          "mode": "REQUIRED",
                          "type": "STRING",
                          "name": "email_address",
                        }
                      ],
                    },
                    "tableReference": {
                      "projectId": gc.project_id,
                      "tableId": new_table_id,
                      "datasetId": dataset_id
                    }
                }

    response = tables.insert(projectId=gc.project_id,
                             datasetId=dataset_id,
                             body=request_body).execute()

    # print out the response
    print(response);

except HttpError as err:
    print 'Error:', err.content
