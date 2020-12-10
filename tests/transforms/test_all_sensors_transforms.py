from caqi.transforms.all_sensors_transforms import drop_columns, rename_columns
import pandas as pd

from pandas.testing import assert_frame_equal

def test_rename_columns():
    input_df = pd.DataFrame([], columns=[
        'THIS_SHOULD_NOT_CHANGE', 'ID', 'ParentID', 'Lat', 'Lon', 'PM2_5Value', 'LastSeen', 'DEVICE_LOCATIONTYPE', 
        'THINGSPEAK_PRIMARY_ID', 'THINGSPEAK_PRIMARY_ID_READ_KEY', 'THINGSPEAK_SECONDARY_ID', 'THINGSPEAK_SECONDARY_ID_READ_KEY',
        'Flag', 'A_H', 'AGE'
    ])

    expected_df = pd.DataFrame([], columns=[
        'THIS_SHOULD_NOT_CHANGE', 'purpleair_id', 'purpleair_parent_id', 'lat', 'lng', 'pm2_5_ug_m3', 'last_seen_epoch_sec', 'location_type',
        'thingspeak_primary_id', 'thingspeak_primary_id_read_key', 'thingspeak_secondary_id', 'thingspeak_secondary_id_read_key',
        'measurement_flagged', 'sensor_downgraded', 'age_mins'
    ])

    actual_df = rename_columns(input_df)

    assert_frame_equal(actual_df, expected_df)

def test_drop_columns():
    input_df = pd.DataFrame([], columns=[
        'THIS_SHOULD_BE_DROPPED', 'purpleair_id', 'purpleair_parent_id', 'lat', 'lng', 'pm2_5_ug_m3', 'last_seen_epoch_sec', 'location_type',
        'thingspeak_primary_id', 'thingspeak_primary_id_read_key', 'thingspeak_secondary_id', 'thingspeak_secondary_id_read_key',
        'measurement_flagged', 'sensor_downgraded', 'age_mins'
    ])

    expected_df = pd.DataFrame([], columns=[
        # Primary
        'purpleair_id', 'purpleair_parent_id', 'lat', 'lng', 'pm2_5_ug_m3', 'last_seen_epoch_sec', 'location_type',
        # Backfill IDs
        'thingspeak_primary_id', 'thingspeak_primary_id_read_key', 'thingspeak_secondary_id', 'thingspeak_secondary_id_read_key',
        # Data Quality
        'measurement_flagged', 'sensor_downgraded', 'age_mins'
    ])

    actual_df = drop_columns(input_df)

    assert_frame_equal(actual_df, expected_df)
