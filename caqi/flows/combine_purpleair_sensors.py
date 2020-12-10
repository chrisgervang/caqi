from prefect.utilities.tasks import task
from caqi.tasks.combine_sensors_tasks import combine_sensors, create_combined_sensors_blob_client, load_combined_sensors
from caqi.tasks.util_tasks import filter_failed
from caqi.tasks.datetime_tasks import datetime_range
from prefect.core.parameter import DateTimeParameter
from caqi.tasks.all_sensors_tasks import create_purpleair_archive_client, extract_warehouse_purpleair_processed
import prefect
from prefect import Flow, Parameter, unmapped

def main():

    with Flow("combine-purpleair-sensors") as flow:
        environment = Parameter("environment", default="staging")
        start = DateTimeParameter("start")
        interval_hour = Parameter("interval_hour", default=1)
        end = DateTimeParameter("end_inclusive")

        dts = datetime_range(start, interval_hour, end)
        client = create_purpleair_archive_client(environment)

        maybe_all_sensors_processed = extract_warehouse_purpleair_processed.map(dt=dts, purpleair_client=unmapped(client))
        all_sensors_processed = filter_failed(maybe_all_sensors_processed)

        combined_sensors = combine_sensors(all_sensors_processed)
        
        blob_client = create_combined_sensors_blob_client(environment)
        load_combined_sensors(combined_sensors, blob_client)

    # Registers flow to server, which we can then deploy and run in background agents.
    # flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    from datetime import datetime
    flow.run(start=datetime(2020, 11, 23, 7), end_inclusive=datetime(2020, 11, 25, 8))

if __name__ == "__main__":
    main()