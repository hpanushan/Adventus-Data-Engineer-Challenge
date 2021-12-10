from google.cloud import bigquery
import json

def create_dataset(project_id,dataset_name):

    # construct a BigQuery client object.
    client = bigquery.Client()

    # dataset_id = "{}.your_dataset".format(client.project)
    dataset_id = project_id + '.' + dataset_name

    # construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # todo(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.

    try:
        # make an API request.
        dataset = client.create_dataset(dataset, timeout=30)  
        print('created dataset')
    except:
        print('error! could not create dataset')

    

def create_table(project_id,dataset_name,table_name):

    # construct a BigQuery client object.
    client = bigquery.Client()

    # table_id = "your-project.your_dataset.your_table_name"
    table_id = project_id + '.' + dataset_name + '.' + table_name

    # define schema
    schema = []
    
    table = bigquery.Table(table_id, schema=schema)

    try:
        # make an API request.
        table = client.create_table(table)  
        print('created table')
    except:
        print('error! could not create table')

    

def load_data(data_file_path,project_id,dataset_name,table_name):

    # construct a BigQuery client object.
    client = bigquery.Client()

    # table_id = "your-project.your_dataset.your_table_name"
    table_id = project_id + '.' + dataset_name + '.' + table_name

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)

    try:
        with open(data_file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        # waits for the job to complete
        job.result() 
        print('data loaded')
    except:
        print('error! could not load data')

    
    
def main():
    # read config values
    config = json.load(open('config.json'))

    data_file_path = config['data_file_1_path']
    project_id = config['project_id']
    dataset_name = config['dataset_name']
    table_name = config['table_name']

    # create new dataset
    create_dataset(project_id,dataset_name)

    # create empty new table
    create_table(project_id,dataset_name,table_name)

    # load data
    load_data(data_file_path,project_id,dataset_name,table_name)

    
if __name__ == '__main__':
    main()

