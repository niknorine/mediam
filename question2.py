from google.cloud import bigquery
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import os
import operator


# use one of the methods below to setup the credentials and fetch the datasets (For this example I will be using method 1

# ---METHOD 1----
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = <PATH_TO_CREDENTIALS>
credentials = GoogleCredentials.get_application_default()
service = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)

response = service.projects().list().execute()
projects = response.get('projects', [])

table_sizes = {}

for project in projects:
    client = bigquery.Client(project['projectId'])

    # list dataset
    datasets = list(client.list_datasets())

# ---METHOD 2----
# client = bigquery.Client.from_service_account_json(<PATH_TO_JSON>)
# datasets = list(client.list_datasets())

    if datasets: # check if there are any datasets in the current project
        for dataset in datasets:
            print(' - {}'.format(dataset.dataset_id))
            get_size = client.query("select table_id, size_bytes as size from "+dataset.dataset_id+".__TABLES__") # this query will output the table_id and size in bytes
            tables = get_size.result()
            for table in tables:
                # project_dataset_table_name can be put into the object as separate values to make it easier to see the sizes in a certain project or dataset. For simplicity, it is one value.
                project_dataset_table_name = project['projectId'] + "." + dataset.dataset_id + "." + table.table_id
                table_sizes[project_dataset_table_name] = table.size

    else:
        print('there are no datasets in {}'.format(project.projectId))

# order the dict based on the size
table_sizes_ordered = sorted(table_sizes.items(), key=operator.itemgetter(1), reverse=True)
print(table_sizes_ordered)
