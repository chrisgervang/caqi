from datetime import datetime
from typing import Any, Dict
from caqi.clients.purpleair_client import PurpleAirClient
from dataclasses import InitVar, dataclass, field
import pandas as pd

@dataclass
class AllSensorsRawDao:
    json_: Dict[str, Any]
    dt: datetime
    
    @classmethod
    def of_live(cls, purpleair_client: PurpleAirClient):
        json_ = purpleair_client.get_live_records()
        dt = datetime.utcnow()
        return cls(json_=json_, dt=dt)

    @classmethod
    def of_archive(cls, dt: datetime, purpleair_client: PurpleAirClient):
        json_ = purpleair_client.get_archived_records(dt)
        return cls(json_=json_, dt=dt)

    def get_version(self):
        return {'map_version': self.json_['mapVersion'], 'base_version': self.json_['baseVersion']}

    def get_records(self):
        return self.json_['results']
    
    def get_json(self):
        return self.json_

if __name__ == "__main__":
    from caqi.clients.purpleair_client import PurpleAirHttpClient, PurpleAirFileSystemClient
    # client = PurpleAirHttpClient()
    client = PurpleAirFileSystemClient()
    dao = AllSensorsRawDao(purpleair_client=client)
    print(dao.get_version())
