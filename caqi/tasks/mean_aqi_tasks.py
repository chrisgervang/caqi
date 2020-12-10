from caqi.clients.file_system_client import FileSystemClient
from caqi.daos.mean_aqi_dao import MeanAqiDao
from prefect import task

@task
def transform_processed_mean(all_sensors_processed):
    return MeanAqiDao.of_all_sensors_processed_dao(all_sensors_processed)

@task
def combine_mean_aqis(mean_aqis):
    return MeanAqiDao.of_mean_aqis(mean_aqis=mean_aqis)

@task
def create_mean_aqi_blob_client(environment):
    return FileSystemClient(sub_path=f'{environment}/mean_aqi')

@task
def load_mean_aqi(mean_aqi_dao, blob_client):
    blob_client.save_csv(mean_aqi_dao.df, "mean_aqi")
