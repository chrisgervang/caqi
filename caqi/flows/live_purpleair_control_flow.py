from caqi.daos.all_sensors_raw_dao import AllSensorsRawDao
from caqi.clients.purpleair_client import PurpleAirHttpClient, PurpleAirFileSystemClient
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.control_flow import ifelse, merge

logger = prefect.context.get("logger")

@task
def extract_offline_live_purpleair():
    client = PurpleAirFileSystemClient()
    all_sensors = AllSensorsRawDao(purpleair_client=client)    
    logger.info(f"PurpleAir JSON Version: {all_sensors.get_version()}")
    return all_sensors

@task
def extract_online_live_purpleair():
    client = PurpleAirHttpClient()
    all_sensors = AllSensorsRawDao(purpleair_client=client)    
    logger.info(f"PurpleAir JSON Version: {all_sensors.get_version()}")
    return all_sensors

def main():
    with Flow("manual-live-purpleair-control-flow") as flow:
        offline = Parameter("offline", default=True)
        all_sensors_online = extract_online_live_purpleair()
        all_sensors_offline = extract_offline_live_purpleair()
        ifelse(offline, all_sensors_offline, all_sensors_online)
        all_sensors = merge(all_sensors_offline, all_sensors_online)

    # Registers flow to server, which we can then deploy and run in background agents.
    flow.register(project_name="caqi-flows")

    # Immediately executes without agents
    # flow.run()

if __name__ == "__main__":
    main()