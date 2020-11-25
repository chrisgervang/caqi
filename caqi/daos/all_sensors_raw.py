from typing import Any, Dict
from caqi.clients.purpleair_client import PurpleAirClient
from dataclasses import InitVar, dataclass, field
import pandas as pd

@dataclass
class AllSensorsRawDao:
    json: Dict[str, Any] = None
    df: pd.DataFrame = None
    purpleair_client: InitVar[PurpleAirClient] = None

    def __post_init__(self, purpleair_client: PurpleAirClient):
        '''
        Pursue self-hydration with provided clients or skip if fields provided.
        '''
        if purpleair_client is not None and self.json is None:
            self.json = purpleair_client.get_live_records()
        
        # if self.df is None and self.json is not None:
        #     self.df = pd.DataFrame(self.json['results'])
        # if self.df is None or self.json is None:
        #     missing_fields = []
        #     if self.df is None:
        #         missing_fields.append('df')
        #     if self.json is None:
        #         missing_fields.append('json')
        #     raise TypeError(f"Required field(s) are None: {missing_fields}")
        if self.json is None:
            raise TypeError(f"Required field is None: 'json'")

    def get_version(self):
        return {'map_version': self.json['mapVersion'], 'base_version': self.json['baseVersion']}

    def get_records(self):
        return self.json['results']
    
    def get_json(self):
        return self.json

if __name__ == "__main__":
    from caqi.clients.purpleair_client import PurpleAirHttpClient, PurpleAirFileSystemClient
    # client = PurpleAirHttpClient()
    client = PurpleAirFileSystemClient()
    dao = AllSensorsRawDao(purpleair_client=client)
    print(dao.get_version())
