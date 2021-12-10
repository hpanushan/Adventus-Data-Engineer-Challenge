from google.cloud import bigquery
import pandas as pd
import json


def load_data(data_file_path,project_id,dataset_name,table_name):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # table_id = "your-project.your_dataset.your_table_name"
    table_id = project_id + '.' + dataset_name + '.' + table_name

    job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        # bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
        # Indexes are written if included in the schema by name.
        # bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
    )

    try:
        # read json data file
        df = pd.read_json(data_file_path,lines=True)

        # make an API request
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)   

        # wait for the job to complete
        job.result() 

        print('data loaded')
    except:
        print('error! could not load data')


def main():
    # read config values
    config = json.load(open('config.json'))

    data_file_path = config['data_file_2_path']
    project_id = config['project_id']
    dataset_name = config['dataset_name']
    table_name = config['table_name']

    load_data(data_file_path,project_id,dataset_name,table_name)


if __name__ == '__main__':
    main()

