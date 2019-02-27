"""Dash-based dashboard"""

import asyncio
import functools
import json
import time
import threading
import logging
from datetime import datetime

import networkx
import jinja2
import humanfriendly

import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash_dangerously_set_inner_html import DangerouslySetInnerHTML

from . import scheduler
from .tasks import SDPPhysicalTaskBase


def timestamp_utc(timestamp):
    t = datetime.utcfromtimestamp(timestamp)
    return t.strftime("%Y-%m-%d %H:%M:%S UTC")


def timespan(delta):
    if delta is not None:
        return humanfriendly.format_timespan(delta)
    else:
        return delta


JINJA_ENV = jinja2.Environment(loader=jinja2.PackageLoader('katsdpcontroller'),
                               autoescape=True, trim_blocks=True)
JINJA_ENV.filters['timestamp_utc'] = timestamp_utc
JINJA_ENV.filters['timespan'] = timespan


def _get_tasks(product):
    order_graph = scheduler.subgraph(product.physical_graph, scheduler.DEPENDS_READY)
    tasks = networkx.lexicographical_topological_sort(
            order_graph.reverse(), key=lambda node: node.name)
    tasks = [task for task in tasks if isinstance(task, SDPPhysicalTaskBase)]
    return tasks


def _get_batch_tasks(product):
    tasks = []
    for name, capture_block in sorted(product.capture_blocks.items()):
        graph = capture_block.postprocess_physical_graph
        if graph is not None:
            for task in graph.nodes:
                if isinstance(task, SDPPhysicalTaskBase):
                    tasks.append((name, task))
    return tasks


def _make_task_details(tasks, active_cell):
    if active_cell is None:
        return []
    row = active_cell[0]
    if not 0 <= row < len(tasks):
        return []
    task = tasks[row]
    # TODO: once the template has stabilised, load it at startup.
    # For now it's very convenient to be able to edit it without
    # restarting the master controller.
    template = JINJA_ENV.get_template('task_details.html.j2')
    value = template.render(task=task, now=time.time())
    return DangerouslySetInnerHTML(value)


class Dashboard:
    def __init__(self, sdp_controller):
        self._sdp_controller = sdp_controller
        self._app = self._make_app()
        logging.getLogger('werkzeug').setLevel(logging.WARNING)

    def _use_event_loop(self, func, *args, **kwargs):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            loop = self._sdp_controller.loop
            future = asyncio.run_coroutine_threadsafe(func(*args, **kwargs), loop)
            return future.result()
        return wrapper

    def _make_app(self):
        sdp_controller = self._sdp_controller
        use_event_loop = self._use_event_loop
        app = dash.Dash(__name__)
        app.css.config.serve_locally = True
        app.scripts.config.serve_locally = True
        app.title = 'SDP Master Controller'
        app.layout = html.Div([
            dcc.Interval(id='interval', interval=1000),    # 1s updates
            html.P('There are no subarray products currently configured',
                   id='no-subarray-products'),
            dcc.Tabs(id='subarray-product-tabs'),
            html.Div(id='subarray-product-content', children=[
                html.P(html.Strong(id='subarray-product-state')),
                dcc.Tabs(id='subarray-product-subtabs', children=[
                    dcc.Tab(label='Tasks', children=html.Div([
                        dash_table.DataTable(
                            id='task-table',
                            columns=[{'name': 'Name', 'id': 'name'},
                                     {'name': 'State', 'id': 'state'},
                                     {'name': 'Mesos State', 'id': 'mesos-state'},
                                     {'name': 'Host', 'id': 'host'}],
                            style_cell={'textAlign': 'left'},
                            sorting=True),
                        html.Div(id='task-details')
                    ])),
                    dcc.Tab(label='Config', children=html.Pre(id='subarray-product-config')),
                    dcc.Tab(label='Capture blocks', children=html.Div([
                        dash_table.DataTable(
                            id='capture-block-table',
                            columns=[{'name': 'ID', 'id': 'name'},
                                     {'name': 'State', 'id': 'state'}],
                            style_cell={'textAlign': 'left'},
                            sorting=True)
                    ])),
                    dcc.Tab(label='Batch jobs', children=html.Div([
                        dash_table.DataTable(
                            id='batch-table',
                            columns=[{'name': 'Name', 'id': 'name'},
                                     {'name': 'Capture Block', 'id': 'capture_block_id'},
                                     {'name': 'State', 'id': 'state'},
                                     {'name': 'Mesos State', 'id': 'mesos-state'},
                                     {'name': 'Host', 'id': 'host'},
                                     {'name': 'Runtime', 'id': 'runtime'}],
                            style_cell={'textAlign': 'left'},
                            sorting=True),
                        html.Div(id='batch-details')
                    ]))
                ])
            ])
        ])

        @app.callback(Output('no-subarray-products', 'style'),
                      [Input('interval', 'n_intervals')])
        @use_event_loop
        async def hide_no_subarray_products(n_intervals):
            """Hide the "no subarray products" message when not applicable"""
            if sdp_controller.subarray_products:
                return {'display': 'none'}
            else:
                return {}

        # Hide the subarray-product-content div when nothing selected
        @app.callback(Output('subarray-product-content', 'style'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def hide_subarray_product_content(product_name, n_intervals):
            if sdp_controller.subarray_products.get(product_name) is None:
                return {'display': 'none'}
            else:
                return {}

        @app.callback(Output('subarray-product-tabs', 'children'),
                      [Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_tabs(n_intervals):
            return [dcc.Tab(label=product, value=product)
                    for product in sorted(sdp_controller.subarray_products.keys())]

        @app.callback(Output('subarray-product-state', 'children'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_subarray_product_state(product_name, n_intervals):
            product = sdp_controller.subarray_products.get(product_name)
            if product is None:
                return ''
            return product.state.name

        @app.callback(Output('task-table', 'data'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_task_table(product_name, n_intervals):
            product = sdp_controller.subarray_products.get(product_name)
            if product is None:
                return {}
            tasks = _get_tasks(product)
            data = [
                {
                    'name': task.logical_node.name,
                    'state': task.state.name,
                    'mesos-state': task.status.state if task.status else '-',
                    'host': task.agent.host if task.agent else '-'
                } for task in tasks
            ]
            return data

        @app.callback(Output('task-details', 'children'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('task-table', 'active_cell'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_task_details(product_name, active_cell, n_intervals):
            product = sdp_controller.subarray_products.get(product_name)
            if product is None:
                return []
            return _make_task_details(_get_tasks(product), active_cell)

        @app.callback(Output('subarray-product-config', 'children'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_subarray_product_config(product_name, n_intervals):
            product = sdp_controller.subarray_products.get(product_name)
            if product is None:
                return ''
            return json.dumps(product.config, indent=2, sort_keys=True)

        @app.callback(Output('capture-block-table', 'data'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_capture_block_table(product_name, n_intervals):
            product = sdp_controller.subarray_products.get(product_name)
            if product is None:
                return []
            return [{'name': name, 'state': capture_block.state.name}
                    for name, capture_block in sorted(product.capture_blocks.items())]

        @app.callback(Output('batch-table', 'data'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_batch_table(product_name, n_intervals):
            product = sdp_controller.subarray_products.get(product_name)
            if product is None:
                return []
            tasks = _get_batch_tasks(product)
            now = time.time()
            data = [
                {
                    'capture_block_id': capture_block_id,
                    'name': task.logical_node.name,
                    'state': task.state.name,
                    'mesos-state': task.status.state if task.status else '-',
                    'host': task.agent.host if task.agent else '-',
                    'runtime':
                        humanfriendly.format_timespan(now - task.start_time)
                        if task.start_time is not None else '-'
                } for (capture_block_id, task) in tasks
            ]
            return data

        @app.callback(Output('batch-details', 'children'),
                      [Input('subarray-product-tabs', 'value'),
                       Input('batch-table', 'active_cell'),
                       Input('interval', 'n_intervals')])
        @use_event_loop
        async def make_batch_details(product_name, active_cell, n_intervals):
            product = sdp_controller.subarray_products.get(product_name)
            if product is None:
                return []
            tasks = [task for (capture_block_id, task) in _get_batch_tasks(product)]
            return _make_task_details(tasks, active_cell)

        return app

    def start(self, port):
        thread = threading.Thread(target=self._app.run_server,
                                  kwargs={'port': port, 'host': '0.0.0.0'})
        thread.daemon = True
        thread.start()
