"""Fakes some of the implementation of Hubspot Singularity.

It is not intended to be a general-purpose mock: it implements the bare
minimum functionality to run the master controller unit tests.

Apart from the HTTP interface, it has methods to manipulate the state from
the test.
"""

import asyncio
import enum
import uuid
from collections import deque
from abc import abstractmethod
from typing import List, Dict, Deque, Mapping, Callable, Awaitable, Optional, Any, TypeVar

import aiohttp.web
import aiohttp.test_utils


_E = TypeVar('_E', bound=enum.Enum)
Lifecycle = Callable[['Task'], Awaitable[None]]


class TaskState(enum.Enum):
    NOT_CREATED = 'notCreated'    # To model Singularity taking time to create the task
    PENDING = 'pending'
    NOT_YET_HEALTHY = 'notYetHealthy'
    HEALTHY = 'healthy'
    CLEANING = 'cleaning'
    DEAD = 'dead'                 # Not reported by Singularity, but a useful internal state


def _next_enum(x : _E) -> _E:
    items = list(type(x))
    return items[items.index(x) + 1]


class _Request:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.request_id: str = config['id']
        self.config = config
        self.active_deploy: Optional['_Deploy'] = None
        self.deploys: Dict[str, '_Deploy'] = {}
        self.tasks: Dict[str, 'Task'] = {}    # Indexed by run_id

    def task_ids(self) -> Dict[str, List[Dict[str, Any]]]:
        ans: Dict[str, List[Dict[str, Any]]] = {
            "notYetHealthy": [],
            "cleaning": [],
            "pending": [],
            "healthy": []
        }
        for task in self.tasks.values():
            if task.state.value in ans:
                ans[task.state.value].append(task.short_info())
        return ans

    def info(self) -> Dict[str, Any]:
        ans = {
            "taskIds": self.task_ids(),
            "request": self.config
        }
        if self.active_deploy is not None:
            ans["activeDeploy"] = self.active_deploy.config
        return ans

    def short_info(self) -> Dict[str, Any]:
        return {"request": self.config}


class _Deploy:
    def __init__(self, request: _Request, config: Dict[str, Any]) -> None:
        self.deploy_id: str = config['id']
        self.request = request
        self.config = config


class Task:
    def __init__(self, deploy: _Deploy, config: Dict[str, Any]) -> None:
        self.deploy = deploy
        self.run_id: str = config['runId']
        self.pending_task_id = uuid.uuid4().hex
        self.task_id = uuid.uuid4().hex
        self.state = TaskState.NOT_CREATED
        self.config = config

        self.killed = asyncio.Event()
        self.force_killed = asyncio.Event()
        self.task_id_known = asyncio.Event()

    @property
    def visible(self) -> bool:
        return self.state in {TaskState.HEALTHY, TaskState.NOT_YET_HEALTHY, TaskState.CLEANING}

    def short_info(self) -> Dict[str, Any]:
        if self.state in {TaskState.NOT_CREATED, TaskState.DEAD}:
            return {}                   # Generally shouldn't be called
        elif self.state == TaskState.PENDING:
            return {}   # TODO
        else:
            return {
                "id": self.task_id,
                "requestId": self.deploy.request.request_id,
                "deployId": self.deploy.deploy_id,
                "host": "fakehost"
            }

    def info(self) -> Dict[str, Any]:
        if self.state in {TaskState.NOT_CREATED, TaskState.DEAD}:
            return {}                   # Generally shouldn't be called
        elif self.state == TaskState.PENDING:
            return {}   # TODO
        else:
            return {
                "taskId": self.short_info(),
                "taskRequest": {
                    "request": self.deploy.request.config,
                    "pendingTask": {
                        "runId": self.run_id
                    }
                },
                "mesosTask": {
                    "command": {
                        "environment": {
                            "variables": [
                                {"name": "TASK_HOST", "value": "slave.invalid"},
                                {"name": "PORT", "value": "12345"},
                                {"name": "PORT0", "value": "12345"},
                                {"name": "PORT1", "value": "12346"}
                            ]
                        }
                    }
                }
            }

    def kill(self, force: bool = False) -> None:
        self.killed.set()
        if force:
            self.force_killed.set()


async def default_lifecycle(task: Task,
                            times: Optional[Mapping[TaskState, Optional[float]]] = None) -> None:
    """Runs task through the full lifecycle, pausing for some time in each state.

    Parameters
    ----------
    task
        Task to run
    times
        The time to spend in each state. If not specified, defaults to 10 seconds,
        except for :const:`TaskState.HEALTHY` which is only interrupted by
        killing. Times may also be ``None`` to never leave the state.
    """
    try:
        times = dict(times) if times is not None else {}
        for state in TaskState:
            if state not in times:
                times[state] = None if state == TaskState.HEALTHY else 10.0

        while not task.force_killed.is_set() and task.state != TaskState.DEAD:
            state = task.state
            if state == TaskState.CLEANING:
                event = task.force_killed
            else:
                event = task.killed

            try:
                await asyncio.wait_for(event.wait(), timeout=times[state])
            except asyncio.TimeoutError:
                task.state = _next_enum(state)
            else:
                if state in {TaskState.NOT_YET_HEALTHY, TaskState.HEALTHY}:
                    task.state = TaskState.CLEANING
                else:
                    task.state = TaskState.DEAD
    finally:
        task.state = TaskState.DEAD


class SingularityServer:
    def __init__(self) -> None:
        app = aiohttp.web.Application()
        app.add_routes([
            aiohttp.web.get('/api/requests/request/{request_id}', self._get_request),
            aiohttp.web.get('/api/requests', self._get_requests),
            aiohttp.web.post('/api/requests', self._create_request),
            aiohttp.web.post('/api/deploys', self._create_deploy),
            aiohttp.web.post('/api/requests/request/{request_id}/run', self._create_run),
            aiohttp.web.get('/api/tasks/task/{task_id}', self._get_task),
            #aiohttp.web.delete('/api/tasks/task/{task_id}', self._delete_task),
            aiohttp.web.get('/api/tasks/ids/request/{request_id}', self._get_request_tasks),
            aiohttp.web.get('/api/track/run/{request_id}/{run_id}', self._track_run)
        ])
        self.server = aiohttp.test_utils.TestServer(app)
        # Each time a task is created, the next class is taken from this deque and
        # used to construct it's lifecycle controller.
        self.lifecycles: Deque[Lifecycle] = deque()
        self._requests: Dict[str, _Request] = {}
        self._tasks: Dict[str, Task] = {}    # Indexed by task ID

    async def _get_request(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        request_id = http_request.match_info['request_id']
        request = self._requests[request_id]
        if request is None:
            raise aiohttp.web.HTTPNotFound
        return aiohttp.web.json_response(request.info())

    async def _get_requests(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        info = [request.short_info() for request in self._requests.values()]
        return aiohttp.web.json_response(info)

    async def _create_request(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        config = await http_request.json()
        request_id = config['id']
        if request_id not in self._requests:
            self._requests[request_id] = _Request(config)
        else:
            self._requests[request_id].config = config
        return aiohttp.web.json_response({})

    async def _create_deploy(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        config = (await http_request.json())['deploy']
        request_id = config['requestId']
        deploy_id = config['id']
        request = self._requests.get(request_id)
        if request is None:
            raise aiohttp.web.HTTPNotFound
        if deploy_id in request.deploys:
            raise aiohttp.web.HTTPBadRequest(
                text='Can not deploy a deploy that has already been deployed')
        request.deploys[deploy_id] = request.active_deploy = _Deploy(request, config)
        return aiohttp.web.json_response({})

    async def _create_run(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        request_id = http_request.match_info['request_id']
        request = self._requests.get(request_id)
        if request is None:
            raise aiohttp.web.HTTPNotFound
        config = await http_request.json()
        run_id = config['runId']
        if any(task.run_id == run_id for task in self._tasks.values()):
            # This is actually legal in Singularity, but complicates matters
            # and not what we want master controller to be doing.
            raise aiohttp.web.HTTPBadRequest(text='Duplicate runId')
        if request.active_deploy is None:
            raise aiohttp.web.HTTPConflict
        task = Task(request.active_deploy, config)
        try:
            lifecycle = self.lifecycles.popleft()
        except IndexError:
            lifecycle = default_lifecycle
        asyncio.ensure_future(lifecycle(task))
        request.tasks[run_id] = task
        self._tasks[task.task_id] = task
        return aiohttp.web.json_response({})

    async def _get_task(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        task_id = http_request.match_info['task_id']
        task = self._tasks.get(task_id)
        if task is None or not task.visible:
            raise aiohttp.web.HTTPNotFound
        return aiohttp.web.json_response(task.info())

    async def _get_request_tasks(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        request_id = http_request.match_info['request_id']
        request = self._requests.get(request_id)
        if request is None:
            raise aiohttp.web.HTTPNotFound
        return aiohttp.web.json_response(request.task_ids())

    async def _track_run(self, http_request: aiohttp.web.Request) -> aiohttp.web.Response:
        request_id = http_request.match_info['request_id']
        run_id = http_request.match_info['run_id']
        request = self._requests.get(request_id)
        if request is None:
            raise aiohttp.web.HTTPNotFound
        task = request.tasks.get(run_id)
        if task is None or task.state == TaskState.NOT_CREATED:
            raise aiohttp.web.HTTPNotFound
        if task.visible:
            data = {
                "taskId": {"id": task.task_id},
                "currentState": "TASK_RUNNING",
                "pending": False
            }
            task.task_id_known.set()
        elif task.state == TaskState.DEAD:
            data = {
                "taskId": {"id": task.task_id},
                "currentState": "TASK_KILLED",
                "pending": False
            }
            task.task_id_known.set()
        elif task.state == TaskState.PENDING:
            data = {
                "runId": run_id,
                "pending": True,
                "pendingTaskId": {
                    "id": task.pending_task_id,
                    "deployId": task.deploy.deploy_id,
                    "requestId": task.deploy.request.request_id,
                    "pendingType": "ONEOFF"
                }
            }
        else:
            data = {}    # TODO
        return aiohttp.web.json_response(data)

    async def start(self) -> None:
        await self.server.start_server()

    async def close(self) -> None:
        await self.server.close()
        for task in self._tasks.values():
            task.kill(force=True)

    @property
    def root_url(self) -> str:
        return str(self.server.make_url('/'))
