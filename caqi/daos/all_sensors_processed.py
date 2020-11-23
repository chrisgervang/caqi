from typing import List
from caqi.daos.all_sensors_raw import AllSensorsRawDao
from dataclasses import InitVar, dataclass
import pandas as pd

@dataclass
class AllSensorsProcessedDao:
    raw_df: pd.DataFrame = None
    all_sensors_raw: InitVar[AllSensorsRawDao] = None

    # kept_columns: List[str] = [
    #     "ID","pm","pm_cf_1","pm_atm","age","pm_0","pm_1","pm_2","pm_3","pm_4","pm_5","pm_6","conf","pm1","pm_10","p1","p2","p3","p4","p5","p6","Humidity","Temperature","Pressure","Elevation","Type","Label","Lat","Lon","Icon","isOwner","Flags","Voc","Ozone1","Adc","CH"
    # ]

    def __post_init__(self, all_sensors_raw: AllSensorsRawDao):
        '''
        Pursue self-hydration with provided raw data dao or skip if processed fields provided.
        '''
        if all_sensors_raw is not None and self.raw_df is None:
            self.raw_df = pd.DataFrame(all_sensors_raw.get_records())

        if self.raw_df is None:
            raise TypeError(f"Required field is None: 'raw_df'")

    
    def get_raw_df(self):
        return self.raw_df