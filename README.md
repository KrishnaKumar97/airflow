##SETUP AIRFLOW

1.  install from pypi using pip
 
    `pip install apache-airflow`
    
2. initialize the database

    `airflow initdb`
    ~~~~
##Install BigQuery

1. Install BigQuery using the following command:
    
   `pip install google-cloud-bigquery`
   
2. Register the project on BigQuery and create a new Service Account.

3. Download the JSON file.

4.  export GOOGLE_APPLICATION_CREDENTIALS to point to the downloaded json file using the command:
    `export GOOGLE_APPLICATION_CREDENTIALS="/home/nineleaps/Downloads/airflow-c4b286984791.json"` 
    
## Steps to be followed before running the application:

1. Copy and paste the main.py file into the dags folder inside your airflow.

    `home/ninleaps/airflow/dags`

2. Create a folder named csvs in which the csv files will be stored.
3. To start the webserver, type:
    
    `airflow webserver -p 8080`

4. Start the scheduler:
    
    `airflow scheduler`
 
5. Open the browser and navigate to localhost:8080 to monitor the DAG.