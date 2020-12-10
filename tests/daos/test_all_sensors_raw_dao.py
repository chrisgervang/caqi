
from caqi.daos.all_sensors_raw_dao import AllSensorsRawDao
import time_machine
from unittest.mock import patch

@time_machine.travel("1955-11-05 01:22")
@patch('caqi.clients.purpleair_client.PurpleAirClient')
def test_of_live(mock_purpleair_client):
    # mocks
    instance_purpleair_client = mock_purpleair_client.return_value
    instance_purpleair_client.get_live_records.return_value = {
        'results': ['value'],
        'mapVersion': 1.0,
        'baseVersion': 2.0
    }

    actual_dao = AllSensorsRawDao.of_live(instance_purpleair_client)
    
    assert actual_dao.dt.timestamp() == -446711880
    assert actual_dao.get_version() == {'map_version': 1.0, 'base_version': 2.0}
    assert actual_dao.get_records() == ['value']
