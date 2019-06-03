"""Class for katcp connections that proxies sensors into a server"""

import logging

import aiokatcp


logger = logging.getLogger(__name__)


class PrometheusObserver:
    """Watches a sensor and mirrors updates into a Prometheus Gauge, Counter or Histogram"""
    def __init__(self, sensor, value_metric, status_metric, label_values):
        self._sensor = sensor
        self._old_value = 0.0
        self._old_timestamp = None
        self._value_metric_root = value_metric
        self._status_metric_root = status_metric
        self._value_metric = value_metric.labels(*label_values)
        self._status_metric = status_metric.labels(*label_values)
        self._label_values = tuple(label_values)
        sensor.attach(self)

    def __call__(self, sensor, reading):
        valid = reading.status.valid_value()
        value = float(reading.value) if valid else 0.0
        timestamp = reading.timestamp
        self._status_metric.set(reading.status.value)
        # Detecting the type of the metric is tricky, because Counter and
        # Gauge aren't actually classes (they're functions). So we have to
        # use introspection.
        metric_type = type(self._value_metric).__name__
        if metric_type == 'Gauge':
            if valid:
                self._value_metric.set(value)
        elif metric_type == 'Counter':
            # If the sensor is invalid, then the counter isn't increasing
            if valid:
                if value < self._old_value:
                    logger.debug(
                        'Counter %s went backwards (%d to %d), not sending delta to Prometheus',
                        self._sensor.name, self._old_value, value)
                    # self._old_value is still updated with value. This is
                    # appropriate if the counter was reset (e.g. at the
                    # start of a new observation), so that Prometheus
                    # sees a cumulative count.
                else:
                    self._value_metric.inc(value - self._old_value)
        elif metric_type == 'Histogram':
            if valid and timestamp != self._old_timestamp:
                self._value_metric.observe(value)
        else:
            raise TypeError('Expected a Counter, Gauge or Histogram, not {}'.format(metric_type))
        if valid:
            self._old_value = value
        self._old_timestamp = timestamp

    def close(self):
        """Shut down observing"""
        self._sensor.detach(self)
        self._status_metric_root.remove(*self._label_values)
        self._value_metric_root.remove(*self._label_values)


class SensorWatcher(aiokatcp.SensorWatcher):
    def __init__(self, client, server, prefix,
                 prometheus_sensors, prometheus_labels, prometheus_factory):
        super().__init__(client)
        self.prefix = prefix
        self.server = server
        self.prometheus_sensors = prometheus_sensors
        self.prometheus_labels = prometheus_labels
        if prometheus_factory is None:
            prometheus_factory = lambda name, sensor: (None, None)     # noqa: E731
        self.prometheus_factory = prometheus_factory
        # Indexed by unqualified sensor name; None if no observer is needed.
        # Invariant: _observers has an entry if and only if the corresponding
        # sensor exists in self.server.sensors
        self._observers = {}
        # Whether we need to do an interface-changed at the end of the batch
        self._interface_changed = False

    def rewrite_name(self, name):
        return self.prefix + name

    def _make_observer(self, name, sensor):
        """Make a :class:`PrometheusObserver` for client sensor `name`, if appropriate.
        Otherwise returns ``None``.
        """
        normalised_name = name.replace(".", "_").replace("-", "_")
        try:
            value_metric, status_metric = self.prometheus_sensors[normalised_name]
        except KeyError:
            value_metric, status_metric = self.prometheus_factory(normalised_name, sensor)
            if value_metric is None:
                return None
            self.prometheus_sensors[normalised_name] = (value_metric, status_metric)
        return PrometheusObserver(sensor, value_metric, status_metric, self.prometheus_labels)

    def sensor_added(self, name, *args):
        """Add a new or replaced sensor with unqualified name `name`."""
        super().sensor_added(name, *args)
        sensor = self.sensors[self.rewrite_name(name)]
        self.server.sensors.add(sensor)
        old_observer = self._observers.get(name)
        if old_observer is not None:
            old_observer.close()
        self._observers[name] = self._make_observer(name, sensor)
        self._interface_changed = True

    def sensor_removed(self, name):
        super().sensor_removed(name)
        rewritten = self.rewrite_name(name)
        self.server.sensors.pop(rewritten, None)
        observer = self._observers.pop(rewritten, None)
        if observer is not None:
            observer.close()
        self._interface_changed = True

    def batch_stop(self):
        super().batch_stop()
        if self._interface_changed:
            self.server.mass_inform('interface-changed', 'sensor-list')
        self._interface_changed = False


class SensorProxyClient(aiokatcp.Client):
    """Client that mirrors sensors into a device server and Prometheus gauges.

    Parameters
    ----------
    server : :class:`aiokatcp.DeviceServer`
        Server to which sensors will be added
    prefix : str
        String prepended to the remote server's sensor names to obtain names
        used on `server`. These should be unique per `server` to avoid
        collisions.
    prometheus_sensors : dict
        Dict mapping sensor names (after underscore normalisation) to pairs of
        Prometheus metrics. These should be integer, float or boolean sensors (TODO:
        enforce this). The first metric must be a Gauge or Counter and receives
        the sensor value. The second must be a Gauge and receives the katcp
        status value. Note that this dictionary is *modified* if a
        `prometheus_factory` is specified.
    prometheus_labels : list
        Labels to apply to the metrics in `prometheus_sensors`.
    prometheus_factory : callable
        Used to dynamically add new entries to `prometheus_sensors`. It is
        called with a normalised name and a sensor and must return either
        ``None, None``, or a pair of metrics as for `prometheus_sensors`. This
        argument may be ``None`` to not specify a factory.
    args, kwargs
        Passed to the base class
    """
    def __init__(self, server, prefix,
                 prometheus_sensors, prometheus_labels, prometheus_factory,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        watcher = SensorWatcher(self, server, prefix,
                                prometheus_sensors, prometheus_labels, prometheus_factory)
        self._synced = watcher.synced
        self.add_sensor_watcher(watcher)

    async def wait_synced(self):
        await self._synced.wait()
