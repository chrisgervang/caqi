
import pandas as pd
from caqi.transforms.all_sensors_transforms import impute_aqi_column
from datetime import datetime
from caqi.daos.all_sensors_processed_dao import AllSensorsProcessedDao
import h3

def transform_mean_aqi(df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
    df = calc_outside_mean(df)
    df = impute_aqi_column(df)
    df = add_dt_columns(df, dt)
    df = df.reset_index()
    df = convert_h3_str_column(df)
    return df

def calc_outside_mean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.query("location_type == 'outside'")
    df = df[['pm2_5_ug_m3', 'h3_9']]
    df = df.groupby('h3_9').mean()
    return df

def add_dt_columns(df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
    df['hour'] = dt.hour
    df['timestamp'] = dt.timestamp()
    return df

def convert_h3_str_column(df: pd.DataFrame) -> pd.DataFrame:
    df['h3_9'] = df['h3_9'].map(h3.h3_to_string)
    return df

if __name__ == "__main__":
    from caqi.clients.purpleair_client import PurpleAirFileSystemClient
    client = PurpleAirFileSystemClient()
    processed_dao = AllSensorsProcessedDao.of_archive_csv(dt=datetime(2020, 11, 25,12), purpleair_client=client)
    ## NOTE: Comparing dtypes of fresh vs csv df
    # from caqi.daos.all_sensors_raw_dao import AllSensorsRawDao
    # raw_dao = AllSensorsRawDao.of_archive(dt=datetime(2020, 11, 25,12), purpleair_client=client)
    # new_processed_dao = AllSensorsProcessedDao.of_raw_dao(all_sensors_raw=raw_dao)
    # print(new_processed_dao.get_processed_df().info())
    # print(processed_dao.get_processed_df().info())
    # print(processed_dao.get_processed_df().dtypes == new_processed_dao.get_processed_df().dtypes)

   

