"""Class for katcp connections that proxies sensors into a server"""

from __future__ import print_function, division, absolute_import
import logging
import katcp
from katcp.inspecting_client import InspectingClientAsync
import tornado.gen
from prometheus_client import Gauge, REGISTRY


logger = logging.getLogger(__name__)


class PrometheusSensor(katcp.Sensor):
    """Sensor that mirrors values into a Prometheus Gauge"""
    def __init__(self, sensor_type, name, description=None, units='',
                 params=None, default=None, initial_status=None,
                 prometheus_name=None):
        super(PrometheusSensor, self).__init__(sensor_type, name, description, units,
                                               params, default, initial_status)
        if prometheus_name is None:
            prometheus_name = name
        try:
            self._gauge = Gauge(prometheus_name, description)
        except ValueError as e:
            logger.warn("Prometheus Gauge %s already exists - not adding again. (%s)", prom_name, e)
            self._gauge = None

    def set_formatted(self, timestamp, status, value, katcp_major):
        super(PrometheusSensor, self).set_formatted(timestamp, status, value, katcp_major)
        if self._gauge is not None:
            timestamp, status, value = self.read()
            if status in [self.NOMINAL, self.WARN, self.ERROR]:
                self._gauge.set(value)


class PrometheusObserver(object):
    """Watches a sensor and mirrors updates into a Prometheus Gauge"""
    def __init__(self, sensor, gauge_name):
        self._sensor = sensor
        try:
            self._gauge = Gauge(gauge_name, sensor.description)
        except ValueError as e:
            logger.warn("Prometheus Gauge %s already exists - not adding again. (%s)", gauge_name, e)
            self._gauge = None
        sensor.attach(self)

    def update(self, sensor, reading):
        if self._gauge is not None:
            if reading.status in [katcp.Sensor.NOMINAL, katcp.Sensor.WARN, katcp.Sensor.ERROR]:
                self._gauge.set(reading.value)

    def close(self):
        """Shut down observing and deregister the gauge"""
        self._sensor.detach(self)
        if self._gauge is not None:
            REGISTRY.unregister(self._gauge)
            self._gauge = None


class SensorProxyClient(InspectingClientAsync):
    def __init__(self, server, prefix, prometheus_sensors, *args, **kwargs):
        super(SensorProxyClient, self).__init__(*args, **kwargs)
        self.server = server
        self.prefix = prefix
        self.prometheus_sensors = set(prometheus_sensors)
        self.set_state_callback(self._sensor_state_cb)
        self.sensor_factory = self._sensor_factory_prefix
        self._observers = {}   #: Dictionary indexed by unqualified sensor name

    def qualify_name(self, name):
        return self.prefix + name

    def _sensor_factory_prefix(self, sensor_type, name, description, units, params):
        return katcp.Sensor(sensor_type, self.qualify_name(name), description, units, params)

    @tornado.gen.coroutine
    def _sensor_state_cb(self, state, model_changes):
        if model_changes is None:
            return
        sensor_changes = model_changes.get('sensors')
        if sensor_changes is None:
            return
        for name in sensor_changes.removed:
            self.server.remove_sensor(self.qualify_name(name))
            observer = self._observers.pop(name, None)
            if observer is not None:
                observer.close()
        for name in sensor_changes.added:
            sensor = yield self.future_get_sensor(name)
            self.server.add_sensor(sensor)
            if name in self.prometheus_sensors:
                prom_name = sensor.name.replace(".","_").replace("-","_")
                self._observers[name] = PrometheusObserver(sensor, prom_name)
        self.server.mass_inform(katcp.Message.inform('interface-changed', 'sensor-list'))

    def stop(self, timeout=None):
        for observer in self._observers.itervalues():
            observer.close()
        self._observers = {}
        super(SensorProxyClient, self).stop(timeout)
