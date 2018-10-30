"""Bokeh-based dashboard"""

import functools

from bokeh.application.handlers.handler import Handler
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import Tabs, Panel, DataTable, TableColumn
from bokeh.layouts import widgetbox


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

    It records the sensors it is attached to, and detached from them
    when :meth:`close` is called.
    """
    def __init__(self):
        self._attachments = []

    def attach_sensor(self, sensor, callback, *, call_now=False):
        sensor.attach(callback)
        self._attachments.append((sensor, callback))
        if call_now:
            callback(sensor, sensor.reading)

    def close(self):
        while self._attachments:
            sensor, callback = self._attachments.pop()
            sensor.detach(callback)


class SubarrayProduct(SensorWatcher):
    """A single subarray-product within :class:`Session`"""
    def __init__(self, session, product):
        super().__init__()
        self.session = session
        self.product = product

        self._task_indices = {}
        data = {'name': [], 'state': []}
        # TODO: sort by dependencies
        for task in product.physical_graph:
            state_name = task.name + '.state'
            if state_name in session.sdp_controller.sensors:
                self._task_indices[task.name] = len(data['name'])
                data['name'].append(task.name)
                data['state'].append(task.state.name)
                self.attach_sensor(session.sdp_controller.sensors[state_name],
                                   self._task_state_changed)
        self._task_cds = ColumnDataSource(data)
        columns = [
            TableColumn(field='name', title='Name'),
            TableColumn(field='state', title='State')
        ]
        task_table = DataTable(
            source=self._task_cds,
            columns=columns,
            index_position=None)

        self.panel = Panel(child=task_table, title=product.subarray_product_id)

    @property
    def session_context(self):
        return self.session.session_context

    @lock_document
    def _task_state_changed(self, doc, sensor, reading):
        assert sensor.name.endswith('.state')
        name = sensor.name[:-6]
        idx = self._task_indices[name]
        self._task_cds.patch({'state': [(idx, reading.value.name)]})


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
        self.attach_sensor(self.sdp_controller.sensors['products'],
                           self._products_changed, call_now=True)

    def modify_document(self, doc):
        doc.add_root(widgetbox(self._product_tabs))

    @lock_document
    def _products_changed(self, doc, sensor, reading):
        products = self.sdp_controller.subarray_products
        for name in products:
            if name not in self._products:
                self._products[name] = SubarrayProduct(self, products[name])
        for name in list(self._products):
            if name not in products:
                self._products[name].close()
                del self._products[name]

        update_tabs(self._product_tabs,
                    [product.panel for name, product in sorted(self._products.items())])


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
