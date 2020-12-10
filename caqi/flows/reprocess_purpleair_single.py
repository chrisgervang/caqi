from prefect.core.parameter import DateTimeParameter
from caqi.tasks.all_sensors_tasks import create_hour_blob_client, create_purpleair_archive_client, extract_warehouse_purpleair, load_all_sensors_processed, transform_all_sensors_raw
import prefect
from prefect import Flow, Parameter

logger = prefect.context.get("logger")
    
def main():

    with Flow("reprocess-purpleair-single") as flow:
        environment = Parameter("environment", default="staging")
        dt = DateTimeParameter("dt")
        client = create_purpleair_archive_client(environment)
        all_sensors_raw = extract_warehouse_purpleair(dt=dt, purpleair_client=client)
        all_sensors_processed = transform_all_sensors_raw(all_sensors_raw)
        blob_client = create_hour_blob_client(environment=environment, dt=dt)
        load_all_sensors_processed(all_sensors_processed, blob_client)

    # Registers flow to server, which we can then deploy and run in background agents.
    flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    # flow.run(start=datetime(2020, 11, 23, 7), end_inclusive=datetime(2020, 11, 25, 8))

if __name__ == "__main__":
    main()