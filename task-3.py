from google.cloud import bigquery
import json
import pandas as pd

# note :- this is based on streaming inserts. Which is not available in free tier

def load_data(data_file_path,project_id,dataset_name,table_name):

    # construct a BigQuery client object.
    client = bigquery.Client()

    # read json data file
    df = pd.read_json(data_file_path,lines=True)

    # convert to lisy of dict
    list_of_dict = df.to_dict('records')

    # table_id = "your-project.your_dataset.your_table_name"
    table_id = project_id + '.' + dataset_name + '.' + table_name

    # make an API request.
    errors = client.insert_rows_json(table_id, list_of_dict)

    if errors == []:
        print("new rows have been added")
    else:
        print("encountered errors while inserting rows: {}".format(errors))


def main():
    # read config values
    config = json.load(open('config.json'))

    data_file_path = config['data_file_3_path']
    project_id = config['project_id']
    dataset_name = config['dataset_name']
    table_name = config['table_name']

    load_data(data_file_path,project_id,dataset_name,table_name)

if __name__ == '__main__':
    main()
