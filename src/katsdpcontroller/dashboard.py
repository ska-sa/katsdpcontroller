"""Dash-based dashboard"""

import asyncio
import functools
import json
import logging
import threading
import time
from datetime import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import jinja2
import networkx
from dash.dependencies import Input, Output
from dash_dangerously_set_inner_html import DangerouslySetInnerHTML

from . import scheduler
from .tasks import ProductPhysicalTask


def timestamp_utc(timestamp):
    t = datetime.utcfromtimestamp(timestamp)
    return t.strftime("%Y-%m-%d %H:%M:%S UTC")


def timespan(delta):
    if delta is not None:
        delta = round(delta)
        return f"{delta // 3600}:{delta // 60 % 60:02}:{delta % 60:02}"
    else:
        return delta


JINJA_ENV = jinja2.Environment(
    loader=jinja2.PackageLoader("katsdpcontroller"), autoescape=True, trim_blocks=True
)
JINJA_ENV.filters["timestamp_utc"] = timestamp_utc
JINJA_ENV.filters["timespan"] = timespan


def _get_tasks(product):
    order_graph = scheduler.subgraph(product.physical_graph, scheduler.DEPENDS_READY)
    tasks = networkx.lexicographical_topological_sort(
        order_graph.reverse(), key=lambda node: node.name
    )
    tasks = [task for task in tasks if isinstance(task, ProductPhysicalTask)]
    return tasks


def _get_batch_tasks(product):
    tasks = []
    for name, capture_block in sorted(product.capture_blocks.items()):
        graph = capture_block.postprocess_physical_graph
        if graph is not None:
            for task in graph.nodes:
                if isinstance(task, ProductPhysicalTask):
                    tasks.append((name, task))
    return tasks


def _get_task_config(product, task):
    """Get the effective telstate config for a task.

    This also looks for nodes whose name indicates that they are parents
    (and hence whose telstate config will be picked up by
    katsdpservices.argparse).
    """
    by_name = {t.logical_node.name: t for t in product.physical_graph}
    conf = {}
    name_parts = task.logical_node.name.split(".")
    for i in range(1, len(name_parts) + 1):
        name = ".".join(name_parts[:i])
        try:
            sub_conf = by_name[name].task_config
        except (KeyError, AttributeError):
            pass
        else:
            conf.update(sub_conf)
    return conf


def _make_task_details(product, tasks, active_cell):
    if active_cell is None or "row_id" not in active_cell:
        return []
    task_name = active_cell["row_id"]
    tasks = [task for task in tasks if task.name == task_name]
    if len(tasks) != 1:
        return []
    task = tasks[0]
    # TODO: once the template has stabilised, load it at startup.
    # For now it's very convenient to be able to edit it without
    # restarting the master controller.
    template = JINJA_ENV.get_template("task_details.html.j2")
    value = template.render(task=task, task_config=_get_task_config(product, task), now=time.time())
    return DangerouslySetInnerHTML(value)


class Dashboard:
    def __init__(self, sdp_controller, **dash_args):
        self._sdp_controller = sdp_controller
        self._app = self._make_app(**dash_args)
        logging.getLogger("werkzeug").setLevel(logging.WARNING)

    def _use_event_loop(self, func, *args, **kwargs):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            loop = self._sdp_controller.loop
            future = asyncio.run_coroutine_threadsafe(func(*args, **kwargs), loop)
            return future.result()

        return wrapper

    def _make_app(self, **dash_args):
        sdp_controller = self._sdp_controller
        use_event_loop = self._use_event_loop
        app = dash.Dash(__name__, **dash_args)
        app.title = "SDP Product Controller"
        app.layout = html.Div(
            id="root",
            children=[
                dcc.Interval(id="interval", interval=1000),  # 1s updates
                html.P(
                    "Lost connection to server (product may have been deconfigured)",
                    id="disconnected",
                ),
                html.P(
                    "Waiting for product-configure call ...",
                    id="no-subarray-product",
                    className="connected",
                ),
                html.Div(
                    id="subarray-product-content",
                    className="connected",
                    children=[
                        html.P(html.Strong(id="subarray-product-state")),
                        dcc.Tabs(
                            id="subarray-product-tabs",
                            children=[
                                dcc.Tab(
                                    label="Tasks",
                                    children=html.Div(
                                        [
                                            dash_table.DataTable(
                                                id="task-table",
                                                columns=[
                                                    {"name": "Name", "id": "name"},
                                                    {"name": "State", "id": "state"},
                                                    {"name": "Mesos State", "id": "mesos-state"},
                                                    {"name": "Host", "id": "host"},
                                                ],
                                                style_cell={"textAlign": "left"},
                                                sort_action=True,
                                            ),
                                            html.Div(id="task-details"),
                                        ]
                                    ),
                                ),
                                dcc.Tab(
                                    label="Config", children=html.Pre(id="subarray-product-config")
                                ),
                                dcc.Tab(
                                    label="Capture blocks",
                                    children=html.Div(
                                        [
                                            dash_table.DataTable(
                                                id="capture-block-table",
                                                columns=[
                                                    {"name": "ID", "id": "name"},
                                                    {"name": "State", "id": "state"},
                                                ],
                                                style_cell={"textAlign": "left"},
                                                sort_action=True,
                                            )
                                        ]
                                    ),
                                ),
                                dcc.Tab(
                                    label="Batch jobs",
                                    children=html.Div(
                                        [
                                            dash_table.DataTable(
                                                id="batch-table",
                                                columns=[
                                                    {"name": "Name", "id": "name"},
                                                    {
                                                        "name": "Capture Block",
                                                        "id": "capture_block_id",
                                                    },
                                                    {"name": "State", "id": "state"},
                                                    {"name": "Mesos State", "id": "mesos-state"},
                                                    {"name": "Host", "id": "host"},
                                                    {"name": "Runtime", "id": "runtime"},
                                                ],
                                                style_cell={"textAlign": "left"},
                                                sort_action=True,
                                            ),
                                            html.Div(id="batch-details"),
                                        ]
                                    ),
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

        @app.callback(
            [
                Output("no-subarray-product", "style"),
                Output("subarray-product-content", "style"),
                Output("subarray-product-state", "children"),
                Output("subarray-product-config", "children"),
                Output("task-table", "data"),
                Output("capture-block-table", "data"),
                Output("batch-table", "data"),
                Output("root", "data-dummy"),
            ],  # Just to set loading state on root
            [Input("interval", "n_intervals")],
        )
        @use_event_loop
        async def top_level(n_intervals):
            if sdp_controller.product is None:
                return {}, {"display": "none"}, "", "", [], [], []
            config = json.dumps(sdp_controller.product.config_dict, indent=2, sort_keys=True)

            tasks = _get_tasks(sdp_controller.product)
            task_data = [
                {
                    "id": task.name,
                    "name": task.logical_node.name,
                    "state": task.state.name,
                    "mesos-state": task.status.state if task.status else "-",
                    "host": task.agent.host if task.agent else "-",
                }
                for task in tasks
            ]

            capture_blocks = sdp_controller.product.capture_blocks
            capture_block_data = [
                {"name": name, "state": capture_block.state.name}
                for name, capture_block in sorted(capture_blocks.items())
            ]

            tasks = _get_batch_tasks(sdp_controller.product)
            now = time.time()
            batch_data = [
                {
                    "id": task.name,
                    "capture_block_id": capture_block_id,
                    "name": task.logical_node.name,
                    "state": task.state.name,
                    "mesos-state": task.status.state if task.status else "-",
                    "host": task.agent.host if task.agent else "-",
                    "runtime": timespan((task.end_time or now) - task.start_time)
                    if task.start_time is not None
                    else "-",
                }
                for (capture_block_id, task) in tasks
            ]
            return (
                {"display": "none"},
                {},
                sdp_controller.product.state.name,
                config,
                task_data,
                capture_block_data,
                batch_data,
                "",
            )

        @app.callback(
            Output("task-details", "children"),
            [Input("task-table", "active_cell"), Input("interval", "n_intervals")],
        )
        @use_event_loop
        async def make_task_details(active_cell, n_intervals):
            if sdp_controller.product is None:
                return []
            return _make_task_details(
                sdp_controller.product, _get_tasks(sdp_controller.product), active_cell
            )

        @app.callback(
            Output("batch-details", "children"),
            [Input("batch-table", "active_cell"), Input("interval", "n_intervals")],
        )
        @use_event_loop
        async def make_batch_details(active_cell, n_intervals):
            if sdp_controller.product is None:
                return []
            tasks = [task for (cbid, task) in _get_batch_tasks(sdp_controller.product)]
            return _make_task_details(sdp_controller.product, tasks, active_cell)

        return app

    def start(self, host, port):
        thread = threading.Thread(
            target=self._app.run_server,
            kwargs={"port": port, "host": "0.0.0.0" if not host else host},
        )
        thread.daemon = True
        thread.start()
