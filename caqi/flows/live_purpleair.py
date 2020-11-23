from caqi.daos.all_sensors_processed import AllSensorsProcessedDao
from datetime import datetime, timedelta
import prefect
from prefect.schedules import IntervalSchedule
from prefect import task, Flow, Parameter
from caqi.clients.file_system_client import FileSystemClient
from caqi.daos.all_sensors_raw import AllSensorsRawDao
from caqi.clients.purpleair_client import PurpleAirClient, PurpleAirHttpClient, PurpleAirFileSystemClient

logger = prefect.context.get("logger")

@task
def create_purpleair_client(offline) -> PurpleAirClient:
    if offline:
        return PurpleAirFileSystemClient()
    return PurpleAirHttpClient()

@task
def extract_live_purpleair(purpleair_client):
    all_sensors_raw = AllSensorsRawDao(purpleair_client=purpleair_client)    
    logger.info(f"PurpleAir JSON Version: {all_sensors_raw.get_version()}")
    return all_sensors_raw

@task
def transform_all_sensors_raw(all_sensors_raw):
    all_sensors_processed = AllSensorsProcessedDao(all_sensors_raw=all_sensors_raw)
    return all_sensors_processed

@task
def create_hour_blob_client():
    dt = datetime.utcnow()
    return FileSystemClient(sub_path=f"all_sensors/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}")

@task
def load_all_sensors_raw_json(all_sensors_raw, blob_client):
    all_sensors_raw_json = all_sensors_raw.get_json()
    blob_client.save_json(all_sensors_raw_json, "raw")

@task
def load_all_sensors_processed(all_sensors_processed, blob_client):
    all_sensors_processed_df = all_sensors_processed.get_raw_df()
    blob_client.save_csv(all_sensors_processed_df, "processed")

def main():

    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1), 
        interval=timedelta(hours=1)
    )

    with Flow("live-purpleair", schedule=schedule) as flow:
        offline = Parameter("offline", default=False)
        purpleair_client = create_purpleair_client(offline)
        all_sensors_raw = extract_live_purpleair(purpleair_client)
        all_sensors_processed = transform_all_sensors_raw(all_sensors_raw)
        blob_client = create_hour_blob_client()
        load_all_sensors_raw_json(all_sensors_raw, blob_client)
        load_all_sensors_processed(all_sensors_processed, blob_client)

    # Registers flow to server, which we can then deploy and run in background agents.
    flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    # flow.run()

if __name__ == "__main__":
    main()