
from caqi.clients.file_system_client import FileSystemClient
from caqi.clients.http_client import HttpClient
import logging 
from time import sleep
from typing import Any, Dict
from dataclasses import dataclass, field

'''
http://www.purpleair.com/data.json
"fields":
["ID","pm","pm_cf_1","pm_atm","age","pm_0","pm_1","pm_2","pm_3","pm_4","pm_5","pm_6","conf","pm1","pm_10","p1","p2","p3","p4","p5","p6","Humidity","Temperature","Pressure","Elevation","Type","Label","Lat","Lon","Icon","isOwner","Flags","Voc","Ozone1","Adc","CH"]

http://www.purpleair.com/json
{"ID":14633,"Label":" Hazelwood canary ","DEVICE_LOCATIONTYPE":"outside","THINGSPEAK_PRIMARY_ID":"559921","THINGSPEAK_PRIMARY_ID_READ_KEY":"CU4BQZZ38WO5UJ4C","THINGSPEAK_SECONDARY_ID":"559922","THINGSPEAK_SECONDARY_ID_READ_KEY":"D0YNZ1LM59LL49VQ","Lat":37.275561,"Lon":-121.964134,"PM2_5Value":"8.69","LastSeen":1606012200,"Type":"PMS5003+PMS5003+BME280","Hidden":"false","isOwner":0,"humidity":"32","temp_f":"62","pressure":"1013.0","AGE":1,"Stats":"{\"v\":8.69,\"v1\":8.94,\"v2\":6.28,\"v3\":5.17,\"v4\":6.39,\"v5\":6.19,\"v6\":9.95,\"pm\":8.69,\"lastModified\":1606012200353,\"timeSinceModified\":120047}"}

Filter to single with show:
http://www.purpleair.com/json?show=14633
This returns two results usually that are parent/child relationship. Are there two because there are two sensors in each device? Each result is slightly different from eachother.
Parent/child looks like "ID":14633 in one, and "ParentID":14633 in the other. The parent has no "ParentID"

*Backfill*
ThingSpeak provides data for individual sensors going back in time. 

THINGSPEAK_PRIMARY_ID: 597961
THINGSPEAK_PRIMARY_ID_READ_KEY: JWMJ211M5KY3AGN2
start
https://api.thingspeak.com/channels/597961/fields/2.json?start=2020-11-15 09:03:28&offset=0&round=2&average=10&api_key=JWMJ211M5KY3AGN2

Primary A:
https://api.thingspeak.com/channels/1150248/feed.csv?api_key=796DTKIN795IV0PM&offset=0&average=&round=2&start=2020-10-05%2000:00:00&end=2020-10-05%2023:59:59
Secondary A:
https://api.thingspeak.com/channels/1150249/feed.csv?api_key=FW4HJJJY2NIYWEFF&offset=0&average=&round=2&start=2020-10-02%2000:00:00&end=2020-10-02%2023:59:59

Primary A:
https://api.thingspeak.com/channels/1150250/feed.csv?api_key=E0TH3WFT6EYLAZEU&offset=0&average=&round=2&start=2020-10-04%2000:00:00&end=2020-10-04%2023:59:59
Secondary B:
https://api.thingspeak.com/channels/1150251/feed.csv?api_key=R0SVL33DLV01TQ0Q&offset=0&average=&round=2&start=2020-10-04%2000:00:00&end=2020-10-04%2023:59:59


Converting AQI from PM2.5
https://docs.google.com/document/d/15ijz94dXJ-YAZLi9iZ_RaBwrZ4KtYeCy08goGBwnbCU/edit#heading=h.47kx5k34pty3



- Store 5 minute snapshots of raw sensor data into parquet.
- Store unique sensor catalog, including ThingSpeak keys.
  - How should we maintain the index? Index is an int
- Convert raw sensor data into AQI.
- Quantize average AQI into level 8 h3 grid.

Analyse to create CAQI:
- How often are sensors missing from raw data? Do h3 cells go missing?
- How sparse is the h3 grid?
- 

'''
@dataclass
class PurpleAirClient:
    def get_live_matrix(self, show: int = None) -> Dict[str, Any]:
        raise NotImplementedError("Function not implemented!")
    def get_live_records(self, show: int = None) -> Dict[str, Any]:
        raise NotImplementedError("Function not implemented!")

@dataclass
class PurpleAirHttpClient(PurpleAirClient):
    http_client: HttpClient = field(default_factory=HttpClient)

    def get_live_matrix(self, show: int = None) -> Dict[str, Any]:
        params = {}
        if show is not None:
            params['show'] = str(show)
        return self.http_client.get_call(url="http://www.purpleair.com/data.json", params=params)
    
    def get_live_records(self, show: int = None) -> Dict[str, Any]:
        '''
        show: get a single purpleair sensor index id. If None, get all.
        '''
        params = {}
        if show is not None:
            params['show'] = str(show)
        return self.http_client.get_call(url="http://www.purpleair.com/json", params=params)

@dataclass
class PurpleAirFileSystemClient(PurpleAirClient):
    file_system_client: FileSystemClient = field(default_factory=FileSystemClient)

    def get_live_matrix(self, show: int = None) -> Dict[str, Any]:
        if show is not None:
            return self.file_system_client.load_json('sample/data.json')
        return self.file_system_client.load_json('sample/data.json')
    
    def get_live_records(self, show: int = None) -> Dict[str, Any]:
        '''
        show: get a single purpleair sensor index id. If None, get all.
        '''
        if show is not None:
            return self.file_system_client.load_json('sample/json.json')
        return self.file_system_client.load_json('sample/json.json')

    

if __name__ == "__main__":
    client = PurpleAirHttpClient()
    
    print(client.get_live_matrix()['version'])

    print(client.get_live_records(show=14633))

    print(client.get_live_records()['mapVersion'])