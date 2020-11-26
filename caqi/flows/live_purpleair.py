from caqi.tasks.all_sensors_tasks import create_hour_blob_client, extract_live_purpleair, load_all_sensors_processed, load_all_sensors_raw_json, transform_all_sensors_raw
from datetime import datetime, timedelta
import prefect
from prefect.schedules import IntervalSchedule
from prefect import task, Flow, Parameter
from caqi.clients.purpleair_client import PurpleAirClient, PurpleAirHttpClient, PurpleAirFileSystemClient

logger = prefect.context.get("logger")

@task
def create_purpleair_client(offline) -> PurpleAirClient:
    if offline:
        return PurpleAirFileSystemClient()
    return PurpleAirHttpClient()

def main():

    schedule = IntervalSchedule(
        start_date=datetime(2020, 11, 23),
        # start_date=datetime.utcnow() + timedelta(seconds=1), 
        interval=timedelta(hours=1)
    )

    with Flow("live-purpleair", schedule=schedule) as flow:
        environment = Parameter("environment", default="prod")
        offline = Parameter("offline", default=False)
        purpleair_client = create_purpleair_client(offline)
        all_sensors_raw = extract_live_purpleair(purpleair_client)
        all_sensors_processed = transform_all_sensors_raw(all_sensors_raw)
        blob_client = create_hour_blob_client(environment)
        load_all_sensors_raw_json(all_sensors_raw, blob_client)
        load_all_sensors_processed(all_sensors_processed, blob_client)

    # Registers flow to server, which we can then deploy and run in background agents.
    flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    # flow.run(parameters={'offline': True})

if __name__ == "__main__":
    main()