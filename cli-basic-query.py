#!/usr/bin/env python

''' Basic query '''

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
    # Create request object and query statement
    query_request = service.jobs()
    query_data = {'query':'SELECT * FROM dataset_id.table_name;'}

    # Call BigQuery API
    response = query_request.query(projectId=gc.project_id,
                                   body=query_data).execute()

    for row in response['rows']:
        result_row = []
        for field in row['f']:
            result_row.append(field['v'])
        print ('\t').join(result_row)

except HttpError as err:
    print 'Error:', err.content
