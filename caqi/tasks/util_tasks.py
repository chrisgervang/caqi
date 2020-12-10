import prefect
from prefect import task
from prefect.engine import signals

@task(trigger=prefect.triggers.any_failed)
def filter_failed(maybe_results):
    '''
    Generic task useful to filter out failed tasks from the flow.
    '''
    results = []
    for result in maybe_results:
        if isinstance(result, signals.FAIL):
            continue
        results.append(result)
    return results