import requests
from datetime import date
from socrata.authorization import Authorization
from socrata import Socrata
from os.path import expanduser
import json

# ECDC base url for data file
BASE_URL_FRAGMENT = 'https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-'


### socrata authentication
home = expanduser("~")
with open(home+"/.soda.json") as creds:
    c = json.load(creds)
    username = c["SODA_USERNAME"]
    password = c["SODA_PASSWORD"]
    apptoken = c["SODA_APPTOKEN"]

auth = Authorization(
  'ecdc-covid-19-response.demo.eu.socrata.com',
  username,
  password
)
socrata = Socrata(auth)


def today_date_string:
	return date.today().strftime('%Y-%m-%d')

def get_url_for_today:
	return BASE_URL_FRAGMENT + today_date_string()

def socrata_upload(csv_data, dataset_id, type):
    print(f'uploading {dataset_id}')
    view = socrata.views.lookup(dataset_id)
    if type == 'update':
        revision = view.revisions.create_update_revision()        
    if type == 'replace':
        revision = view.revisions.create_replace_revision()
    upload = revision.create_upload(f'ecdc_data_{today_date_string()}.csv')
    source = upload.csv(csv_data)
    output_schema = source.get_latest_input_schema().get_latest_output_schema()
    job = revision.apply(output_schema=output_schema)
    job = job.wait_for_finish(progress = lambda job: print('Job progress:', job.attributes['status']))

# pull down data
r = requests.get(get_url_for_today())

if r.status_code == 200:
    today_data = r.text    
    socrata_upload(today_data,'bfi2-tewf',type='replace')
else:
    print('Non-200 status code.')