"""Bokeh-based dashboard"""

import functools
import json
import weakref
import time
from datetime import datetime

import networkx
import jinja2
import humanfriendly

from bokeh.application.handlers.handler import Handler
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import Tabs, Panel, DataTable, TableColumn, Div, PreText, Paragraph
from bokeh.layouts import widgetbox, column

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


def lock_document(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        return self.session_context.with_locked_document(
            lambda doc: func(self, doc, *args, **kwargs))

    return wrapper


def update_tabs(tabs, panels):
    """Modify the set of panels in a Tabs widget.

    This works around the problem that inserting or removing a panel to the
    left of the active one will cause the active panel to be changed.
    """
    try:
        old_panel = tabs.tabs[tabs.active]
    except IndexError:
        old_panel = None    # e.g. if tabs was previously empty
    tabs.tabs = panels
    try:
        tabs.active = tabs.tabs.index(old_panel)
    except ValueError:
        pass  # The active panel was removed


class SensorWatcher:
    """Base utility class for reacting to sensor changes

    It records the sensors it is attached to, and detaches from them
    when :meth:`close` is called.
    """
    def __init__(self):
        self._attachments = weakref.WeakKeyDictionary()

    def attach_sensor(self, sensor, callback, *, call_now=False):
        sensor.attach(callback)
        self._attachments.setdefault(sensor, []).append(callback)
        if call_now:
            callback(sensor, sensor.reading)

    def close(self):
        for sensor, callbacks in list(self._attachments.items()):
            for callback in callbacks:
                sensor.detach(callback)
        self._attachments.clear()


class Task:
    """Monitor a single task within a :class:`SubarrayProduct`"""
    def __init__(self, session, task, data_source):
        super().__init__()
        self.session = session
        self.task = task
        self._data_source = data_source
        self._index = len(data_source.data['name'])
        data_source.stream({key: [value] for key, value in self._fields().items()})
        for suffix in ['state', 'mesos-state']:
            sensor_name = task.name + '.' + suffix
            session.attach_sensor(session.sdp_controller.sensors[sensor_name], self._changed)

    @property
    def session_context(self):
        return self.session.session_context

    @lock_document
    def _changed(self, doc, sensor, reading):
        self._data_source.patch({key: [(self._index, value)]
                                 for (key, value) in self._fields().items()})

    def _fields(self):
        task = self.task
        return {
            'name': task.logical_node.name,
            'state': task.state.name,
            'mesos-state': task.status.state if task.status else '-',
            'host': task.agent.host if task.agent else '-'
        }

    def show_details(self, details):
        # TODO: once the template has stabilised, load it at startup.
        # For now it's very convenient to be able to edit it without
        # restarting the master controller.
        template = JINJA_ENV.get_template('task_details.html.j2')
        details.text = template.render(task=self.task, now=time.time())


class SubarrayProduct:
    """A single subarray-product within :class:`Session`"""
    def __init__(self, session, product):
        self.session = session
        self.product = product
        self.name = product.subarray_product_id

        state = self._make_state()
        tabs = widgetbox(Tabs(tabs=[self._make_tasks(),
                                    self._make_config(),
                                    self._make_capture_blocks()]))
        self.panel = Panel(child=column(state, tabs), title=self.name)

    def _get_sensor(self, name):
        full_name = self.name + '.' + name
        return self.session.sdp_controller.sensors[full_name]

    @property
    def session_context(self):
        return self.session.session_context

    def _make_state(self):
        self._state_widget = Paragraph()
        self.session.attach_sensor(self._get_sensor('state'), self._state_changed, call_now=True)
        return widgetbox(self._state_widget)

    @lock_document
    def _state_changed(self, doc, sensor, reading):
        self._state_widget.text = reading.value.name

    def _task_selected(self, attr, old, new):
        if len(new) == 1 and 0 <= new[0] < len(self._tasks):
            self._tasks[new[0]].show_details(self._task_details)
        else:
            self._task_details.text = ''

    def _make_tasks(self):
        self._tasks = []
        self._task_details = Div(text='', width=1000)
        data_source = ColumnDataSource({'name': [], 'state': [], 'mesos-state': [], 'host': []})
        data_source.selected.on_change('indices', self._task_selected)
        order_graph = scheduler.subgraph(self.product.physical_graph, scheduler.DEPENDS_READY)
        for task in networkx.lexicographical_topological_sort(
                order_graph.reverse(), key=lambda node: node.name):
            if isinstance(task, SDPPhysicalTaskBase):
                self._tasks.append(Task(self.session, task, data_source))
        columns = [
            TableColumn(field='name', title='Name', width=400),
            TableColumn(field='state', title='State', width=100),
            TableColumn(field='mesos-state', title='Mesos State', width=150),
            TableColumn(field='host', title='Host', width=200)
        ]
        table = DataTable(
            source=data_source,
            columns=columns, width=1000, height=600,
            index_position=None)
        table.sizing_mode = 'stretch_both'
        return Panel(child=column(table, self._task_details), title='Tasks')

    def _make_capture_blocks(self):
        self._cb_data_source = ColumnDataSource({'name': [], 'state': []})
        columns = [
            TableColumn(field='name', title='ID'),
            TableColumn(field='state', title='State')
        ]
        table = DataTable(
            source=self._cb_data_source,
            columns=columns,
            index_position=None)
        self.session.attach_sensor(self._get_sensor('capture-block-state'),
                                   self._capture_blocks_changed, call_now=True)
        return Panel(child=table, title='Capture blocks')

    @lock_document
    def _capture_blocks_changed(self, doc, sensor, reading):
        data = {'name': [], 'state': []}
        for name, capture_block in sorted(self.product.capture_blocks.items()):
            data['name'].append(name)
            data['state'].append(capture_block.state.name)
        self._cb_data_source.data = data

    def _make_config(self):
        config = json.dumps(self.product.config, indent=2, sort_keys=True)
        pre = widgetbox(PreText(text=config, width=1000, style={'font-size': 'small'}))
        return Panel(child=pre, title='Config')


class Session(SensorWatcher):
    """Wrapper around single bokeh session.

    Bokeh has a separate "session" approximately for each browser tab, each
    with its own "document". Thus, even though we don't really have per-user
    state, we have to replicate updates to all sessions.
    """
    def __init__(self, sdp_controller, session_context):
        super().__init__()
        self.sdp_controller = sdp_controller
        self.session_context = session_context
        self._products = {}
        self._product_tabs = Tabs()
        self._no_products = Paragraph(text='There are no subarray products currently configured')
        self._root = widgetbox(self._no_products)
        self.attach_sensor(self.sdp_controller.sensors['products'],
                           self._products_changed, call_now=True)

    def modify_document(self, doc):
        doc.add_root(self._root)
        doc.title = 'SDP master controller'

    @lock_document
    def _products_changed(self, doc, sensor, reading):
        products = self.sdp_controller.subarray_products
        for name in products:
            if name not in self._products:
                self._products[name] = SubarrayProduct(self, products[name])
        for name in list(self._products):
            if name not in products:
                del self._products[name]

        update_tabs(self._product_tabs,
                    [product.panel for name, product in sorted(self._products.items())])
        if self._products:
            self._root.children = [self._product_tabs]
        else:
            self._root.children = [self._no_products]


class Dashboard(Handler):
    def __init__(self, sdp_controller):
        super().__init__()
        self._sdp_controller = sdp_controller
        self._sessions = {}     # Maps session IDs to instances of Session

    def on_session_destroyed(self, session_context):
        session = self._sessions.pop(session_context.id)
        session.close()

    def modify_document(self, doc):
        session_context = doc.session_context
        session = Session(self._sdp_controller, session_context)
        self._sessions[session_context.id] = session
        session.modify_document(doc)
