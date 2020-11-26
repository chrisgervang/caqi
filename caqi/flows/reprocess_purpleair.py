from prefect.core.parameter import DateTimeParameter
from prefect.utilities.tasks import task
from caqi.tasks.all_sensors import create_hour_blob_client, create_purpleair_archive_client, extract_warehouse_purpleair, load_all_sensors_processed, transform_all_sensors_raw
from datetime import timedelta
import prefect
from prefect import Flow, Parameter, unmapped

logger = prefect.context.get("logger")

@task
def reprocessed_datetimes(start, interval_hour, end):
    current_time = start
    datetimes = []
    while current_time <= end:
        datetimes.append(current_time)
        current_time += timedelta(hours=interval_hour)
    return datetimes
    
def main():

    with Flow("reprocess-purpleair") as flow:
        environment = Parameter("environment", default="staging")
        start = DateTimeParameter("start")
        interval_hour = Parameter("interval_hour", default=1)
        end = DateTimeParameter("end_inclusive")

        expected_dts = reprocessed_datetimes(start, interval_hour, end)
        client = create_purpleair_archive_client(environment)

        all_sensors_raw = extract_warehouse_purpleair.map(dt=expected_dts, purpleair_client=unmapped(client))
        all_sensors_processed = transform_all_sensors_raw.map(all_sensors_raw)
        blob_client = create_hour_blob_client.map(environment=unmapped(environment), dt=expected_dts)
        load_all_sensors_processed.map(all_sensors_processed, blob_client)

    # Registers flow to server, which we can then deploy and run in background agents.
    flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    # flow.run(start=datetime(2020, 11, 23, 7), end_inclusive=datetime(2020, 11, 25, 8))

if __name__ == "__main__":
    main()