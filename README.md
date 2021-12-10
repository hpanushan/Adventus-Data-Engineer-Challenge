# Adventus Data Engineer Challenge
## _BigQuery & Python_

This applications can be used to create new dataset and tables in BigQuery. Then data is loaded to the table by using a job and streaming inserts. 

## Features

- Create a new BigQuery dataset
- Create a new BigQuery table
- Load JSON data to BigQuery table by using a load job
- Load JSON data to BigQuery table by using a streaming inserts (note :- this is not available in GCP free trial)

## Requirements

- GCP project
- Service account key with correct access to BigQuery 
- Python environment


## Installation

Three applications require Python 3.7.0 to run.

1. Set environemnt variable (Replace KEY_PATH with the path of the JSON file that contains your service account key)
    For Windows, 
    ```sh
    set GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"
    ```
    For Linux or Mac,
    ```sh
    export GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"
    ```
    
2. Install the dependencies
    ```sh
    pip install requirements.txt
    ```

## Run Applications

All the three dataset files are inside of "data" fodler. You need to change values in "config.json" before run the applications as replace datasets path properties of config file. 

Task 01 :- 

```sh
python task-1.py
```

Task 02 :- 

```sh
python task-2.py
```

Task 03 :- 

```sh
python task-3.py
```

