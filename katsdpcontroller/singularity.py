"""Wrappers for Hubspot Singularity"""

import json
from typing import Union, Sequence
import logging

import yarl
import aiohttp


class SingularityError(Exception):
    """Base class for wrapped HTTP errors"""


class NotFoundError(SingularityError):
    """HTTP error code 404"""


class ConflictError(SingularityError):
    """HTTP error code 409"""


_CT_JSON = {'Content-type': 'application/json'}
_STATUS_TO_EXCEPTION = {
    404: NotFoundError,
    409: ConflictError
}
logger = logging.getLogger(__name__)


async def _read_resp(resp):
    if 400 <= resp.status < 600:
        msg = await resp.text()
        try:
            resp.raise_for_status()
        except aiohttp.ClientResponseError as exc:
            logger.debug('Error response content: %s', msg)
            if exc.status in _STATUS_TO_EXCEPTION:
                raise _STATUS_TO_EXCEPTION[exc.status]() from exc
            raise
    else:
        return await resp.json()


class Singularity:
    """Wrappers for Hubspot Singularity.

    The methods are thin wrappers around REST endpoints, doing JSON encoding
    and decoding. However, a few HTTP errors (currently 404 and 409) are
    turned into specific exception types to make them easier to catch. Any
    other errors allow :class:`aiohttp.ClientResponseError` to propagate.
    """
    def __init__(self, url: Union[str, yarl.URL]) -> None:
        self._http_session = aiohttp.ClientSession()
        self._url = yarl.URL(url)

    async def close(self) -> None:
        await self._http_session.close()

    async def _post(self, path: str, data: dict, *args, **kwargs) -> dict:
        url = self._url / path
        async with self._http_session.post(url, headers=_CT_JSON,
                                           data=json.dumps(data), *args, **kwargs) as resp:
            return await _read_resp(resp)

    async def _get(self, path: str, *args, **kwargs) -> dict:
        url = self._url / path
        async with self._http_session.get(url, *args, **kwargs) as resp:
            return await _read_resp(resp)

    async def _delete(self, path: str, *args, **kwargs) -> dict:
        url = self._url / path
        async with self._http_session.delete(url, *args, **kwargs) as resp:
            return await _read_resp(resp)

    async def get_request(self, request_id: str) -> dict:
        return await self._get(f'requests/request/{request_id}')

    async def get_requests(self, *, request_type: Union[str, Sequence[str]] = ()) -> dict:
        if isinstance(request_type, str):
            request_type = [request_type]
        params = [('requestType', r) for r in request_type]
        return await self._get('requests', params=params)

    async def create_request(self, request: dict) -> dict:
        return await self._post('requests', request)

    async def create_deploy(self, deploy: dict) -> dict:
        return await self._post('deploys', deploy)

    async def create_run(self, request_id: str, run: dict) -> dict:
        return await self._post(f'requests/request/{request_id}/run', run)

    async def get_task(self, task_id: str) -> dict:
        return await self._get(f'tasks/task/{task_id}')

    async def kill_task(self, task_id: str) -> dict:
        return await self._delete(f'tasks/task/{task_id}')

    async def get_request_tasks(self, request_id: str) -> dict:
        return await self._get(f'tasks/ids/request/{request_id}')

    async def track_run(self, request_id: str, run_id: str) -> dict:
        return await self._get(f'track/run/{request_id}/{run_id}')
