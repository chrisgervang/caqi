
from __future__ import annotations
from dataclasses import dataclass
from typing import List
import pandas as pd
from caqi.daos.all_sensors_processed_dao import AllSensorsProcessedDao
from caqi.transforms.mean_aqi_transforms import transform_mean_aqi

@dataclass
class MeanAqiDao:
    df: pd.DataFrame
    
    @classmethod
    def of_all_sensors_processed_dao(cls, processed_dao: AllSensorsProcessedDao):
        df = processed_dao.get_processed_df()
        df = transform_mean_aqi(df, processed_dao.dt)
        return cls(df)

    @classmethod
    def of_mean_aqis(cls, mean_aqis: List[MeanAqiDao]):
        dfs = [mean_aqi.df for mean_aqi in mean_aqis]
        df = pd.concat(dfs)
        return cls(df)
