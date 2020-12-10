from caqi.clients.file_system_client import FileSystemClient
from caqi.daos.all_sensors_processed_dao import AllSensorsProcessedDao
from prefect import task

@task
def create_combined_sensors_blob_client(environment):
    return FileSystemClient(sub_path=f'{environment}/combined_sensors')

@task
def combine_sensors(all_sensors_processed):
    return AllSensorsProcessedDao.of_processed_sensors(all_sensors_processed)

@task
def load_combined_sensors(combined_all_sensors_dao, blob_client):
    blob_client.save_csv(combined_all_sensors_dao.df, "combined_sensors")
