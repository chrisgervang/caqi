from caqi.tasks.mean_aqi_tasks import combine_mean_aqis, create_mean_aqi_blob_client, load_mean_aqi, transform_processed_mean
from caqi.tasks.util_tasks import filter_failed
from caqi.tasks.datetime_tasks import datetime_range
from caqi.tasks.all_sensors_tasks import create_purpleair_archive_client, extract_warehouse_purpleair_processed
import prefect
from prefect.core.parameter import DateTimeParameter
from prefect import Flow, Parameter, unmapped

logger = prefect.context.get("logger")

def main():

    with Flow("mean-aqi") as flow:
        environment = Parameter("environment", default="staging")
        start = DateTimeParameter("start")
        interval_hour = Parameter("interval_hour", default=1)
        end = DateTimeParameter("end_inclusive")

        dts = datetime_range(start, interval_hour, end)
        client = create_purpleair_archive_client(environment)

        maybe_all_sensors_processed = extract_warehouse_purpleair_processed.map(dt=dts, purpleair_client=unmapped(client))
        all_sensors_processed = filter_failed(maybe_all_sensors_processed)
        
        mean_aqi = transform_processed_mean.map(all_sensors_processed)
        combined_mean_aqi = combine_mean_aqis(mean_aqi)
        blob_client = create_mean_aqi_blob_client(environment)
        load_mean_aqi(combined_mean_aqi, blob_client)
        # blob_client = create_hour_blob_client.map(environment=unmapped(environment), dt=dts)
        # load_all_sensors_processed.map(all_sensors_processed, blob_client)

    # Registers flow to server, which we can then deploy and run in background agents.
    flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    # from datetime import datetime
    # flow.run(start=datetime(2020, 11, 23, 7), end_inclusive=datetime(2020, 11, 26, 7))

if __name__ == "__main__":
    main()