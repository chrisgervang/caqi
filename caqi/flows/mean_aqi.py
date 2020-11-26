from caqi.clients.file_system_client import FileSystemClient
from datetime import datetime
from caqi.daos.mean_aqi_dao import MeanAqiDao
from caqi.daos.all_sensors_processed_dao import AllSensorsProcessedDao
from caqi.tasks.datetime_tasks import datetime_range
from prefect.core.parameter import DateTimeParameter
from caqi.tasks.all_sensors_tasks import create_purpleair_archive_client
import prefect
from prefect import Flow, Parameter, unmapped, task

logger = prefect.context.get("logger")

@task
def extract_warehouse_purpleair_processed(dt, purpleair_client):
    return AllSensorsProcessedDao.of_archive_csv(dt=dt, purpleair_client=purpleair_client)

@task
def transform_processed_mean(all_sensors_processed):
    return MeanAqiDao.of_all_sensors_processed_dao(all_sensors_processed)

@task
def combine_mean_aqis(mean_aqis):
    return MeanAqiDao.of_mean_aqis(mean_aqis=mean_aqis)

@task
def create_mean_aqi_blob_client():
    return FileSystemClient(sub_path='staging/mean_aqi')

@task
def load_mean_aqi(mean_aqi_dao, blob_client):
    blob_client.save_csv(mean_aqi_dao.df, "mean_aqi")

def main():

    with Flow("mean-aqi") as flow:
        environment = Parameter("environment", default="staging")
        start = DateTimeParameter("start")
        interval_hour = Parameter("interval_hour", default=1)
        end = DateTimeParameter("end_inclusive")

        dts = datetime_range(start, interval_hour, end)
        client = create_purpleair_archive_client(environment)

        all_sensors_processed = extract_warehouse_purpleair_processed.map(dt=dts, purpleair_client=unmapped(client))
        mean_aqi = transform_processed_mean.map(all_sensors_processed)
        combined_mean_aqi = combine_mean_aqis(mean_aqi)
        blob_client = create_mean_aqi_blob_client()
        load_mean_aqi(combined_mean_aqi, blob_client)
        # blob_client = create_hour_blob_client.map(environment=unmapped(environment), dt=dts)
        # load_all_sensors_processed.map(all_sensors_processed, blob_client)

    # Registers flow to server, which we can then deploy and run in background agents.
    flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    # flow.run(start=datetime(2020, 11, 23, 7), end_inclusive=datetime(2020, 11, 26, 7))

if __name__ == "__main__":
    main()