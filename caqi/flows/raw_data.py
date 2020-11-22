from caqi.clients.purpleair_client import PurpleAirClient
import prefect
from prefect import task, Flow

@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")

@task
def extract_raw_purpleair():
    logger = prefect.context.get("logger")
    logger.info("Extracting Raw PurpleAir JSON")

    client = PurpleAirClient()
    
    raw_json = client.get_matrix()
    logger.info(f"PurpleAir JSON Version: {raw_json['version']}")


    return raw_json

flow = Flow("hello-flow", tasks=[extract_raw_purpleair])

# Registers flow to server, which we can then deploy and run in background agents.
flow.register(project_name="caqi-flows")

# Immediately executes without agents
# flow.run()