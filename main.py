# Authentication: create a service account, generate a key in JSON format 
# Next save its contents (everything except opening `{` and closing `}`) as google_key secret.
# how-to: https://medium.com/@yu51a5/bigquery-programmatic-access-hello-stack-overflow-data-in-bigquery-world-query-5d0806e146a9

import os
import pandas as pd
import db_dtypes

from google.cloud import bigquery
from google.oauth2 import service_account

assert 'google_key' in os.environ, "Please define a secret called google_key; its value must be the contents of access key in JSON format"

cred = service_account.Credentials.from_service_account_info(eval("{" + os.environ['google_key'] +"}"))
client = bigquery.Client(credentials=cred, project=cred.project_id)
# running a dummy query to make sure that the access key is valid
try:
  query_job = client.query("SELECT * FROM `bigquery-public-data.stackoverflow.posts_questions` LIMIT 1")
  query_job.result()
except:
  raise Exception("Error: your google_key secret is not valid")


# SQL dialect reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions 
for csv_name, tablename in (('data', 'bigquery-public-data.stackoverflow.posts_questions'), 
                            ('fh_data', 'fh-bigquery.stackoverflow_archive_questions.merged')):
  for limit, order in ((2000, 'view_count'), (1, 'creation_date')):
    query_str = f"SELECT id, view_count, creation_date FROM `{tablename}` ORDER BY {order} DESC LIMIT {limit}"
    results = client.query(query_str).to_dataframe()
    if limit > 1:
      results.to_csv(f'{csv_name}_{limit}.csv', sep='€')
    else:
      print(f'The most recent question in {tablename} was created on {results["creation_date"][0]}')

print("all done!")
