from caqi.clients.purpleair_client import PurpleAirClient
from caqi.transforms.all_sensors_transforms import transform_all_sensors
from datetime import datetime
from caqi.daos.all_sensors_raw_dao import AllSensorsRawDao
from dataclasses import dataclass
import pandas as pd

@dataclass
class AllSensorsProcessedDao:
    df: pd.DataFrame = None
    dt: datetime = None
    
    @classmethod
    def of_raw_dao(cls, all_sensors_raw: AllSensorsRawDao):
        df = transform_all_sensors(all_sensors_raw.get_records())
        return cls(df=df, dt=all_sensors_raw.dt)
    
    @classmethod
    def of_archive_csv(cls, dt: datetime, purpleair_client: PurpleAirClient):
        df = purpleair_client.get_archived_processed(dt)
        return cls(df=df, dt=dt)

    def get_processed_df(self) -> pd.DataFrame:
        return self.df

if __name__ == "__main__":
    from caqi.clients.purpleair_client import PurpleAirHttpClient, PurpleAirFileSystemClient
    # client = PurpleAirHttpClient()
    client = PurpleAirFileSystemClient()
    raw_dao = AllSensorsRawDao.of_archive(dt=datetime(2020, 11, 25,12), purpleair_client=client)
    # print(raw_dao.get_records()[0])
    processed_dao = AllSensorsProcessedDao.of_raw_dao(all_sensors_raw=raw_dao)
