import logging
from time import sleep
from typing import Dict
from prefect.utilities.exceptions import PrefectError
import requests
from requests import exceptions as requests_exceptions
from dataclasses import dataclass

def _retryable_error(exception):
    return (
        isinstance(
            exception,
            (requests_exceptions.ConnectionError, requests_exceptions.Timeout),
        )
        or exception.response is not None
        and exception.response.status_code >= 500
    )

@dataclass
class HttpClient:
    retry_limit: int = 3
    retry_delay_secs: float = 5.0
    timeout_secs: int = 180

    def get_call(self, url: str, params: Dict[str, str] = None) -> Dict:
        attempt_num = 1
        while True:
            try:
                response = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout_secs,
                )
                response.raise_for_status()
                return response.json()
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise PrefectError(
                        "Response: {0}, Status Code: {1}".format(
                            e.response.content, e.response.status_code
                        )
                    ) from e

                self._log_request_error(attempt_num, e)

            if attempt_num == self.retry_limit:
                raise PrefectError(
                    f"API requests to PurpleAir failed {self.retry_limit} times. Giving up."
                )

            attempt_num += 1
            sleep(self.retry_delay_secs)
    
    def _log_request_error(self, attempt_num, error):
        logging.error(
            "Attempt %s API Request to Databricks failed with reason: %s",
            attempt_num,
            error,
        )
