from caqi.clients.purpleair_client import PurpleAirClient, PurpleAirFileSystemClient
from datetime import datetime
from prefect import task
import prefect
from caqi.daos.all_sensors_raw_dao import AllSensorsRawDao
from caqi.daos.all_sensors_processed_dao import AllSensorsProcessedDao
from caqi.clients.file_system_client import FileSystemClient
from prefect.engine import signals

logger = prefect.context.get("logger")

## Clients

@task
def create_purpleair_archive_client(environment) -> PurpleAirClient:
    fs_client = FileSystemClient(sub_path=environment)
    return PurpleAirFileSystemClient(fs_client)

@task
def create_hour_blob_client(environment, dt=None) -> FileSystemClient:
    if dt is None:
        dt = datetime.utcnow()
    return FileSystemClient(sub_path=f"{environment}/all_sensors/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}")

## Raw Sensor Tasks

@task
def extract_live_purpleair(purpleair_client):
    all_sensors_raw = AllSensorsRawDao.of_live(purpleair_client=purpleair_client)    
    return all_sensors_raw

@task
def extract_warehouse_purpleair(dt, purpleair_client):
    all_sensors_raw = AllSensorsRawDao.of_archive(dt=dt, purpleair_client=purpleair_client)    
    return all_sensors_raw

@task
def transform_all_sensors_raw(all_sensors_raw):
    all_sensors_processed = AllSensorsProcessedDao.of_raw_dao(all_sensors_raw=all_sensors_raw)
    return all_sensors_processed

@task
def load_all_sensors_raw_json(all_sensors_raw, blob_client):
    all_sensors_raw_json = all_sensors_raw.get_json()
    blob_client.save_json(all_sensors_raw_json, "raw")


## Processed Sensor Tasks

@task
def extract_warehouse_purpleair_processed(dt, purpleair_client):
    try:
        return AllSensorsProcessedDao.of_archive_csv(dt=dt, purpleair_client=purpleair_client)
    except IOError as e:
        raise signals.FAIL(e)

@task
def load_all_sensors_processed(all_sensors_processed, blob_client):
    all_sensors_processed_df = all_sensors_processed.get_processed_df()
    blob_client.save_csv(all_sensors_processed_df, "processed")

