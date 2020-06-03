import csv
from datetime import datetime, timedelta

from google.cloud import bigquery


def create_dataset(client):
    """
    Function to create dataset
    :param client: instance of Bigquery client
    :return: dataset instance
    """
    dataset_id = "{}.airflow".format(client.project)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    return dataset


def create_new_table(client, dataset):
    """
    Function to create table
    :param client: instance of Bigquery client
    :param dataset: instance of dataset
    :return: table instance
    """
    table_id = "{}.{}.corona_cases_table".format(client.project, dataset.dataset_id)
    table = bigquery.Table(table_id)
    table = client.create_table(table, exists_ok=True)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    return table


def insert_data(client, table):
    """
    Function to insert data into table
    :param client: instance of client
    :param table: instance of table
    :return:
    """
    filename = "/home/nineleaps/PycharmProjects/airflow/csvs/{}.csv".format(yesterday_date)
    dataset_ref = client.dataset(table.dataset_id)
    table_ref = dataset_ref.table(table.table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()
    print("Loaded {} rows into {}:{}.".format(job.output_rows, table.dataset_id, table.table_id))

    input_file = open("{}".format(filename), "r+")
    reader_file = csv.reader(input_file)
    value = len(list(reader_file))
    print("Percentage uploaded: {}%".format(job.output_rows * 100 / (value - 1)))


# Driver Program
if __name__ == '__main__':
    yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%d-%b-%y')
    client = bigquery.Client()
    dataset = create_dataset(client)
    table = create_new_table(client, dataset)
    insert_data(client, table)
