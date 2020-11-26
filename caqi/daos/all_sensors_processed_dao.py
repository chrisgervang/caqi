from caqi.transforms.all_sensors_transforms import transform_all_sensors
from datetime import datetime
from caqi.daos.all_sensors_raw_dao import AllSensorsRawDao
from dataclasses import dataclass
import pandas as pd

@dataclass
class AllSensorsProcessedDao:
    processed_df: pd.DataFrame = None
    dt: datetime = None
    
    @classmethod
    def of_raw_dao(cls, all_sensors_raw: AllSensorsRawDao):
        processed_df = transform_all_sensors(all_sensors_raw.get_records())
        
        # print(processed_df)
        # print(processed_df.info())
        return cls(processed_df=processed_df, dt=all_sensors_raw.dt)


    def get_processed_df(self) -> pd.DataFrame:
        return self.processed_df

if __name__ == "__main__":
    from caqi.clients.purpleair_client import PurpleAirHttpClient, PurpleAirFileSystemClient
    # client = PurpleAirHttpClient()
    client = PurpleAirFileSystemClient()
    raw_dao = AllSensorsRawDao.of_archive(dt=datetime(2020, 11, 25,12), purpleair_client=client)
    # print(raw_dao.get_records()[0])

    processed_dao = AllSensorsProcessedDao.of_raw_dao(all_sensors_raw=raw_dao)
