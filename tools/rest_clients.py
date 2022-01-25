import logging
from typing import Optional

import requests
from ccmlib.node import Node
from requests import Response


logger = logging.getLogger(__name__)


class StorageServiceClient:
    def __init__(self, node: Node):
        self._node = node
        self._endpoint_url = f"{self._node.address()}:10000/storage_service/"

    def scrub_ks_cf(self, keyspace: str, cf: Optional[str], scrub_mode: Optional[str] = None, **kwargs) -> Response:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_scrub/{keyspace}"

        if scrub_mode:
            params.update({"scrub_mode": scrub_mode})

        response = requests.get(url=self._full_url(path), params=params)
        logger.debug(f"Request url: {response.request.url}")

        return response

    def _full_url(self, path: str):
        return f"{self._endpoint_url}{path}"
