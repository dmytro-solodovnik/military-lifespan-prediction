import request
import json
from datetime import date, timedelta
from google.cloud import bigquery

if __name__ == "__main__":
    # This snippet demonstrates how to load data from some API to BQ.
    # It uses Google Storage for initial load.

    # Variables to setup
    # TODO(developer): Set API URL for the initial load.
    api_url_iteration = 'https://russianwarship.rip/api/v1/statistics/'

    # TODO(developer): Set table_id to the ID of the table to create.
    dataset_id = "atest"
    table_id = "russianwarship"

    # TODO(developer): Set temp file name for data from API.
    file_name = "sample-json-data.json"

    # TODO(developer): Specify start and end date, interval to load.
    start_date = date(2022, 2, 24)
    end_date = date(2022, 11, 3)
    delta = timedelta(days=1)

    # Logic implementation
    # Generate stream of requests
    data_content = []
    while start_date <= end_date:
        print(start_date.strftime("%Y-%m-%d"))
        data_url = api_url_iteration + start_date.strftime("%Y-%m-%d")
        # Request Data from API
        try:
            data = request.request(data_url)
            data_content.append(data['data'])
        except Exception as e:
            print('no data')

        start_date += delta

    # Write API data into the file
    with open(file_name, "w") as jsonwrite:
        for item in data_content:
            jsonwrite.write(json.dumps(item) + '\n')  # newline delimited json file

    # Write data into BQ from the file
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    with open(file_name, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="us",  # Must match the destination dataset location.
            job_config=job_config,
        )  # API request

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
