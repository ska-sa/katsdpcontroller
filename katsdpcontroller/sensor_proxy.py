"""Class for katcp connections that proxies sensors into a server"""

import logging
import katcp
from katcp.inspecting_client import InspectingClientAsync
import tornado.gen
from prometheus_client import Gauge, Counter


logger = logging.getLogger(__name__)


class PrometheusObserver(object):
    """Watches a sensor and mirrors updates into a Prometheus Gauge or Counter"""
    def __init__(self, sensor, value_metric, status_metric):
        self._sensor = sensor
        self._old_value = 0.0
        self._value_metric = value_metric
        self._status_metric = status_metric
        sensor.attach(self)

    def update(self, sensor, reading):
        valid = reading.status in [katcp.Sensor.NOMINAL, katcp.Sensor.WARN, katcp.Sensor.ERROR]
        value = float(reading.value) if valid else 0.0
        self._status_metric.set(reading.status)
        # Detecting the type of the metric is tricky, because Counter and
        # Gauge aren't actually classes (they're functions). So we have to
        # use introspection.
        if type(self._value_metric).__name__ == 'Gauge':
            self._value_metric.set(value)
        elif type(self._value_metric).__name__ == 'Counter':
            # If the sensor is invalid, then the counter isn't increasing
            if valid:
                if value < self._old_value:
                    logger.warn(
                        'Counter %s went backwards (%d to %d), not sending delta to Prometheus',
                        self._sensor.name, self._old_value, value)
                    # self._old_value is still updated with value. This is
                    # appropriate if the counter was reset (e.g. at the
                    # start of a new observation), so that Prometheus
                    # sees a cumulative count.
                else:
                    self._value_metric.inc(value - self._old_value)
        else:
            raise TypeError('Expected a Counter or Gauge')
        if valid:
            self._old_value = value

    def close(self):
        """Shut down observing"""
        self._sensor.detach(self)
        self._status_metric.set(katcp.Sensor.UNREACHABLE)


class SensorProxyClient(InspectingClientAsync):
    """Inspecting client wrapper that mirrors sensors into a device server,
    and optionally into Prometheus gauges as well.

    Parameters
    ----------
    server : :class:`katcp.DeviceServer`
        Server to which sensors will be added
    prefix : str
        String prepended to the remote server's sensor names to obtain names
        used on `server`. These should be unique per `server` to avoid
        collisions.
    prometheus_sensors : dict
        Dict mapping sensor names (after underscore normalisation) to pairs of
        Prometheus metrics. These should be integer or float sensors (TODO:
        enforce this). The first metric must be a Gauge or Counter and receives
        the sensor value. The second must be a Gauge and receives the katcp
        status value.
    prometheus_labels : list
        Labels to apply to the metrics in `prometheus_sensors`.
    args, kwargs
        Passed to the base class
    """
    def __init__(self, server, prefix, prometheus_sensors, prometheus_labels, *args, **kwargs):
        super(SensorProxyClient, self).__init__(*args, **kwargs)
        self.server = server
        self.prefix = prefix
        self.prometheus_sensors = prometheus_sensors
        self.prometheus_labels = prometheus_labels
        self.set_state_callback(self._sensor_state_cb)
        self.sensor_factory = self._sensor_factory_prefix
        # Indexed by unqualified sensor name; None if no observer is needed
        self._observers = {}

    def qualify_name(self, name):
        """Map a remote server sensor name to a local name"""
        return self.prefix + name

    def _sensor_factory_prefix(self, sensor_type, name, description, units, params):
        return katcp.Sensor(sensor_type, self.qualify_name(name), description, units, params)

    @tornado.gen.coroutine
    def _apply_strategy(self, sensor_name):
        reply, informs = yield self.simple_request('sensor-sampling', sensor_name, 'auto')
        if not reply.reply_ok():
            logger.warn('Failed to set sensor strategy on %s: %s', sensor_name, reply.arguments[1])

    @tornado.gen.coroutine
    def _sensor_state_cb(self, state, model_changes):
        if state.data_synced and not state.synced:
            # This is the state immediately after an interface change or the
            # initial connect. It may be overkill to reapply the strategy to
            # all sensors, but it's possible that the device server removed a
            # sensor and replaced it with an identical one, which won't show
            # up in model_changes but will invalidate the strategy.
            for sensor_name in self._observers:
                yield self._apply_strategy(sensor_name)
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
            try:
                normalised_name = name.replace(".", "_").replace("-", "_")
                value_metric, status_metric = self.prometheus_sensors[normalised_name]
                # It's tempting to push this work onto the caller, but that
                # causes the metric to be instantiated with the labels
                # whether the matching sensor exists or not.
                value_metric = value_metric.labels(*self.prometheus_labels)
                status_metric = status_metric.labels(*self.prometheus_labels)
            except KeyError:
                self._observers[name] = None
            else:
                self._observers[name] = PrometheusObserver(sensor, value_metric, status_metric)
            yield self._apply_strategy(name)
        self.server.mass_inform(katcp.Message.inform('interface-changed', 'sensor-list'))

    def stop(self, timeout=None):
        for name, observer in self._observers.items():
            if observer is not None:
                observer.close()
            self.server.remove_sensor(self.qualify_name(name))
        self._observers = {}
        self.server.mass_inform(katcp.Message.inform('interface-changed', 'sensor-list'))
        super(SensorProxyClient, self).stop(timeout)
