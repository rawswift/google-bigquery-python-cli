#!/usr/bin/env python

'''
Basic query
Reference: https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.jobs.html#query
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

    job = service.jobs()

    # Create request object and query statement
    # Replace 'dataset_id.table_name' w/ the actual dataset and table name/id
    request_body = {'query':'SELECT * FROM dataset_id.table_name;'}

    # Call BigQuery API
    response = job.query(projectId=gc.project_id,
                         body=request_body).execute()

    for row in response['rows']:
        result_row = []
        for field in row['f']:
            result_row.append(field['v'])
        print ('\t').join(result_row)

except HttpError as err:
    print 'Error:', err.content
