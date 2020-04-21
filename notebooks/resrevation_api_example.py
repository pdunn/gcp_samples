# -*- coding: utf-8 -*-
"""Copy of resrevation_api_example.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1YqT5HUh3nu3-xHx7YX9Fqz739qXzIcHk

Copyright [2018] [Google]
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

!pip install google-api-python-client

admin_project_id =  ''#'Project that owns the slot capacity'
project_id = ''#'BigQuery Project to use slots'
region = 'US'

"""# Authenticate"""

from google.colab import auth
auth.authenticate_user()

# https://cloud.google.com/resource-manager/docs/creating-managing-projects
!gcloud config set project {project_id}

"""# On Demand Query"""

from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig
import time

def run_query(prefix, dry_run=True):
  client = bigquery.Client(project=project_id)
  config = QueryJobConfig(use_query_cache=False, 
                          use_legacy_sql=False,
                          dry_run=dry_run)
  
  query = '''
    SELECT source_id_mf, single_date, frame_id, count(*) OVER () as total
    FROM `bigquery-public-data.wise_all_sky_data_release.mep_wise` a
    INNER JOIN
  	  (
        SELECT state_geom 
    	  FROM `bigquery-public-data.geo_us_boundaries.states` 
    	  WHERE state = 'VA'
  	  ) b
  	  ON ST_WITHIN(a.point, b.state_geom)
    LIMIT 10000'''

  start = time.time()
  job = client.query(query, job_config=config, job_id_prefix=prefix)

  while (not job.done()):
    time.sleep(2)
  
  end = time.time()

  GB = 1000 * 1000 * 1000
  if not dry_run:
    row_count = job.to_dataframe().total[0]
    print('query took ~{} seconds to return'.format((end - start)))
    print('query result is {}'.format(row_count))
    print('GB processed {}'.format((job.total_bytes_processed / GB)))
  else:
    print('dry_run estimated GB processed {}'.format((job.total_bytes_processed / GB)))

  print('GB billed {}'.format((job.total_bytes_billed / GB)))

run_query("on-demand-")

"""# Build Reservations API
Use Google discovery api to build python client
"""

from apiclient.discovery import build
res_api = build(serviceName='bigqueryreservation', version="v1beta1").projects().locations()
parent_arg = "projects/{}/locations/{}".format(admin_project_id, region)

"""# Commitments, Reservations, and Assignments
Commitment is a purchase, Flex slots in this case<br/>
Reservation is a named allocation of slots<br/>
Assignments give Orgs, Folders, and Projects access to a Reservation
"""

def create_commitment(slots=500):
  commitment_req = {
    'plan':'FLEX',
    'slotCount':slots
  }

  commit = res_api.capacityCommitments()\
             .create(parent=parent_arg, body=commitment_req)\
             .execute()
  print(commit)
  return commit['name']

def create_reservation(reservation_name, slots=500):
  reservation_req = {
    'slotCapacity':slots,
    'ignoreIdleSlots': False
  }
  res = res_api.reservations()\
          .create(parent=parent_arg, reservationId=reservation_name, body=reservation_req)\
          .execute()
  print(res)
  return res['name']

def create_assignment(reservation_id, user_project):
  assignment_req = {
    'assignee':"projects/{}".format(user_project),
    'jobType':"QUERY"
  }

  assignment = res_api.reservations()\
                 .assignments()\
                 .create(parent=reservation_id, body=assignment_req)\
                 .execute()
  print(assignment)
  return assignment['name']

def cleanup(assignment_id, reservation_id, commit_id):
  res_api.reservations()\
    .assignments()\
    .delete(name=assignment_id)\
    .execute()
  res_api.reservations()\
    .delete(name=reservation_id)\
    .execute()
  
  retry = 0
  while retry < 20:
    try:
      res_api.capacityCommitments()\
        .delete(name=commit_id)\
        .execute()
      break
    except:
      retry += 1
      time.sleep(5)

start = time.time()
slots = 500
commit_id = create_commitment(slots)
reservation_name = 'sample-reservation'
res_id = create_reservation(reservation_name, slots)
assign_id = create_assignment(res_id, project_id)
time.sleep(180) #reservation takes a few minutes to kick in
print('start query')
run_query("reserved-", False)
cleanup(assign_id, res_id, commit_id)

end = time.time()
print("reservation ran for ~{} seconds".format((end - start)))