import prefect
from prefect import task, Flow

@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")

flow = Flow("hello-flow", tasks=[hello_task])

# Registers flow to server, which we can then deploy and run in background agents.
flow.register(project_name="caqi-flows")

# Immediately executes without agents
# flow.run()