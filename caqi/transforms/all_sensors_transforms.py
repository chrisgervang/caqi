from caqi.aqi import aqi_from_pm
from typing import Any, Dict
import pandas as pd
import numpy as np

'''
Transforms:

1. Rename Columns
2. Only Keep Used Columns
3. Convert Types
4. Infer Values
5. Drop Bad Rows
6. Impute New Columns
'''

def transform_all_sensors(raw_records: Dict[str, Any]) -> pd.DataFrame:
    processed_df = pd.DataFrame(raw_records)
    processed_df = rename_columns(processed_df)
    processed_df = drop_columns(processed_df)
    processed_df = convert_types(processed_df)
    processed_df = infer_rows(processed_df)
    processed_df = drop_bad_rows(processed_df)
    processed_df = impute_channel_column(processed_df)
    processed_df = impute_h3_9_column(processed_df)
    processed_df = impute_aqi_column(processed_df)
    processed_df = final_drop_columns(processed_df)
    return processed_df

def rename_columns(processed_df: pd.DataFrame) -> pd.DataFrame:
    # 1. Rename Columns
    rename_columns = {
        # Primary
        'ID': 'purpleair_id',
        'ParentID': 'purpleair_parent_id', # null means it is parent and Channel A. Channel B has parent ID.
        'Lat': 'lat', 
        'Lon': 'lng', 
        'PM2_5Value': 'pm2_5_ug_m3', # convert str to float
        'LastSeen': 'last_seen_epoch_sec',
        'DEVICE_LOCATIONTYPE': 'location_type', # null | outside | inside. null means see parent. If has parent, and parent is outside then this sensor is outside.
        # Backfill IDs
        'THINGSPEAK_PRIMARY_ID': 'thingspeak_primary_id', # contains CF=1 values for indoor sensors. (Incorrectly labeled ATM prior to 20 October 2019)
        'THINGSPEAK_PRIMARY_ID_READ_KEY': 'thingspeak_primary_id_read_key', 
        'THINGSPEAK_SECONDARY_ID': 'thingspeak_secondary_id', # contains ATM values for outdoor sensors. (Incorrectly labeled CF=1 prior to 20 October 2019)
        'THINGSPEAK_SECONDARY_ID_READ_KEY': 'thingspeak_secondary_id_read_key',
        # Data Quality
        'Flag': 'measurement_flagged', # Single measurement flagged for unusually high readings. 1 or 0
        'A_H': 'sensor_downgraded', # The sensor output has been downgraded or marked for attention due to suspected hardware issues "true" or null. Independent of "Flag".
        'AGE': 'age_mins' # Sensor data age (when data was last received) in minutes. We wouldn't want to accept measurements older then 2 hours (since we're recording every hour).
    }
    processed_df: pd.DataFrame = processed_df.rename(columns=rename_columns)
    return processed_df

def drop_columns(processed_df: pd.DataFrame) -> pd.DataFrame:
    # 2. Only Keep Used Columns
    keep_columns = [
        # Primary
        'purpleair_id', 'purpleair_parent_id', 'lat', 'lng', 'pm2_5_ug_m3', 'last_seen_epoch_sec', 'location_type',
        # Backfill IDs
        'thingspeak_primary_id', 'thingspeak_primary_id_read_key', 'thingspeak_secondary_id', 'thingspeak_secondary_id_read_key',
        # Data Quality
        'measurement_flagged', 'sensor_downgraded', 'age_mins'
    ]
    return processed_df.filter(items=keep_columns, axis='columns')

def convert_types(processed_df: pd.DataFrame) -> pd.DataFrame:
    # 3. Convert Types    
    processed_df['measurement_flagged'] = processed_df['measurement_flagged'].replace(to_replace={ np.nan: False, 0: False, 1: True})
    processed_df['sensor_downgraded'] = processed_df['sensor_downgraded'].replace(to_replace={ np.nan: False, 'true': True })
    
    ## Some sensors are missing API keys. They are "object" type, so first cast to float32, then to Int32Dtype. 
    processed_df = processed_df.astype({'thingspeak_primary_id': np.float32, 'thingspeak_secondary_id': np.float32})

    column_dtypes = {
        'purpleair_id': np.int32,
        'purpleair_parent_id': pd.Int32Dtype(),
        'pm2_5_ug_m3': np.float32,
        'last_seen_epoch_sec': np.uint64,
        'lat': np.double,
        'lng': np.double,
        'thingspeak_primary_id': pd.Int32Dtype(),
        'thingspeak_secondary_id': pd.Int32Dtype(),
        'age_mins': np.uint32
    }
    processed_df = processed_df.astype(column_dtypes)
    # print(processed_df[processed_df['thingspeak_primary_id'].isna()])
    return processed_df

def convert_types_from_csv(processed_df: pd.DataFrame) -> pd.DataFrame:
    # 3a. Convert Types when deserializing from CSV. NOTE: using parquet preserves types better.

    column_dtypes = {
        'purpleair_id': np.int32,
        'purpleair_parent_id': pd.Int32Dtype(),
        'pm2_5_ug_m3': np.float32,
        'last_seen_epoch_sec': np.uint64,
        'lat': np.double,
        'lng': np.double,
        'thingspeak_primary_id': pd.Int32Dtype(),
        'thingspeak_secondary_id': pd.Int32Dtype(),
        'age_mins': np.uint32,
        'h3_9': np.uint64,
        'location_type': 'category',
        'channel': 'category'
    }
    processed_df = processed_df.astype(column_dtypes)
    # print(processed_df[processed_df['thingspeak_primary_id'].isna()])
    return processed_df

def infer_rows(processed_df: pd.DataFrame) -> pd.DataFrame:
    '''
    4. Infer Values

    location_type: Category[indoor | outside] if null, join on parent's value.

    select 'purpleair_id', 'location_type' where 'location_type' is not null
    create { purpleair_id: location_type } e.g. {14633: 'outside'}
    select location_type (value) where purpleair_id (key) equals purpleair_parent_id
    fill nulls with rows from filled_children_series
    fill remaining NaN with 'unknown' location
    '''
    only_parents_fill_dict = processed_df[['purpleair_id', 'location_type']] \
        .dropna(subset=['location_type']) \
        .set_index('purpleair_id')['location_type'] \
        .to_dict()
    filled_children_series = processed_df['purpleair_parent_id'].map(only_parents_fill_dict) 
    processed_df['location_type'] = processed_df['location_type'] \
        .fillna(filled_children_series) \
        .fillna('unknown') \
        .astype('category')
    return processed_df

def drop_bad_rows(processed_df: pd.DataFrame) -> pd.DataFrame:
    '''
    5. Drop Bad Rows

    if age_mins >= 60, since worst case our hourly data could have missed a new measurement one hour old.
    i.e. 1:00 etl, age very old | 1:01 new measurement, age is 0 | 2:00 etl, age is 59
    if lat is NaN
    if lng is NaN
    if measurement_flagged is true
    if sensor_downgraded is true
    if last_seen_epoch_sec <= datetime.utcnow() - timedelta(hours=2)
    if pm2_5_ug_m3 is NaN
    '''
    freshness_threshold_mins = 60

    '''
    TODO: problem: utcnow wont work for back processing, so drop using 'age' for now.
    # data: 1606012200 
    recent_datetime = datetime.utcnow() - timedelta(minutes=freshness_threshold_mins)
    recent_timestamp = int(recent_datetime.timestamp())
    print(recent_timestamp)
    #> 1606307881 
    query_string = 'last_seen_epoch_sec < @recent_timestamp'
    '''
    # uncomment to inspect filtered rows.
    # processed_df_bad_rows = processed_df.query(
    #     'pm2_5_ug_m3.isnull() or age_mins > @freshness_threshold_mins or measurement_flagged or sensor_downgraded'
    # )
    # print(processed_df_bad_rows)

    processed_df = processed_df.query(
        f'pm2_5_ug_m3.notnull() and lat.notnull() and lng.notnull() and age_mins <= {freshness_threshold_mins} and not measurement_flagged and not sensor_downgraded'
    )
    processed_df = processed_df.reset_index(drop=True)
    return processed_df

'''
6. Impute New Columns

channel: Category[A | B] (if it did or did not have a parent)
h3_9: Use int64 based on lat/lng
aqi: int based on pm2_5_ug_m3
TODO?: pm2_5_type: Category[CF=1 | ATM] (if indoor, CF=1 else ATM)
'''
def impute_channel_column(processed_df: pd.DataFrame) -> pd.DataFrame:
    ## all A is NaN
    processed_df['channel'] = processed_df['purpleair_parent_id'].replace({pd.NA: 'A'})
    ## anything that isn't A is B
    channel = processed_df['channel'].copy()
    channel[channel != 'A'] = 'B'
    processed_df['channel'] = channel.astype('category')
    return processed_df

def impute_h3_9_column(processed_df: pd.DataFrame) -> pd.DataFrame:
    # print(processed_df[processed_df['lat'].isna()])
    lat = processed_df['lat'].to_numpy()
    lng = processed_df['lng'].to_numpy()
    from h3.unstable.vect import geo_to_h3
    h3_9 = geo_to_h3(lat, lng, 9)
    processed_df['h3_9'] = h3_9
    return processed_df

def impute_aqi_column(processed_df: pd.DataFrame) -> pd.DataFrame:
    processed_df['aqi'] = processed_df['pm2_5_ug_m3'].map(aqi_from_pm)
    return processed_df

def final_drop_columns(processed_df: pd.DataFrame) -> pd.DataFrame:
    '''
    7. Final Drop

    measurement_flagged: all values are false
    sensor_downgraded: all values are False
    TODO?: ParentID: unused after channel determined.
    '''
    return processed_df.drop(columns=['measurement_flagged', 'sensor_downgraded'])

'''
NOTE
"pm","pm_cf_1","pm_atm"

ATM is "atmospheric", meant to be used for outdoor applications
CF=1 is meant to be used for indoor or controlled environment applications

Original (incorrect) assumption corrected 20 October 2019: PurpleAir uses CF=1 values on the map. This value is lower than the ATM value in higher measured concentrations.

It'd be useful to label the model: CF=1 or ATM.
'''