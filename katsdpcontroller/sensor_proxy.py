"""Class for katcp connections that proxies sensors into a server"""

import logging
from typing import Tuple, Dict, Callable, Mapping, Iterable, Union, Optional

import aiokatcp
import prometheus_client       # noqa: F401
from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry


logger = logging.getLogger(__name__)
_Metric = Union[Gauge, Counter, Histogram]
# Use a string so that code won't completely break if Prometheus internals change.
# The Union is required because mypy doesn't like pure strings being used indirectly.
_LabelWrapper = Union['prometheus_client.core._LabelWrapper']
_Factory = Callable[[str, aiokatcp.Sensor], Optional['PrometheusInfo']]


def _dummy_factory(name: str, sensor: aiokatcp.Sensor) -> None:
    return None


class PrometheusInfo:
    """Specify information for creating a Prometheus series from a katcp sensor.

    Parameters
    ----------
    class_
        Callable that produces the Prometheus metric from `name`, `description` and labels
    name
        Name of the Prometheus metric
    description
        Description for the Prometheus metric
    labels
        Labels to combine with `name` to produce the series.
    """

    def __init__(self, class_: Callable[..., 'prometheus_client.core._LabelWrapper'],
                 name: str, description: str,
                 labels: Mapping[str, str],
                 registry: CollectorRegistry = prometheus_client.REGISTRY) -> None:
        self.class_ = class_
        self.name = name
        self.description = description
        self.labels = dict(labels)
        self.registry = registry


class PrometheusObserver:
    """Watches a sensor and mirrors updates into a Prometheus Gauge, Counter or Histogram"""
    def __init__(self, sensor: aiokatcp.Sensor,
                 value_metric: _LabelWrapper,
                 status_metric: _LabelWrapper,
                 label_values: Iterable[str]) -> None:
        self._sensor = sensor
        self._old_value = 0.0
        self._old_timestamp: Optional[float] = None
        self._value_metric_root = value_metric
        self._status_metric_root = status_metric
        self._value_metric: _Metric = value_metric.labels(*label_values)
        self._status_metric: _Metric = status_metric.labels(*label_values)
        self._label_values = tuple(label_values)
        sensor.attach(self)

    def __call__(self, sensor: aiokatcp.Sensor, reading: aiokatcp.Reading) -> None:
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

    def close(self) -> None:
        """Shut down observing"""
        self._sensor.detach(self)
        self._status_metric_root.remove(*self._label_values)
        self._value_metric_root.remove(*self._label_values)


class SensorWatcher(aiokatcp.SensorWatcher):
    """Mirrors sensors from a client into a server and optionally Prometheus.

    See :class:`SensorProxyClient` for an explanation of the parameters.
    """

    # Caches metrics by name. Each entry stores the primary metric
    # and the status metric.
    prometheus_sensors: Dict[str, Tuple[_LabelWrapper, _LabelWrapper]] = {}

    def __init__(self, client: aiokatcp.Client, server: aiokatcp.DeviceServer,
                 prefix: str,
                 prometheus_labels: Mapping[str, str] = None,
                 prometheus_factory: _Factory = None,
                 prometheus_sensors: Dict[str, Tuple[_LabelWrapper, _LabelWrapper]] = None,
                 rewrite_gui_urls: Callable[[aiokatcp.Sensor], bytes] = None) -> None:
        super().__init__(client)
        self.prefix = prefix
        self.server = server
        # We keep track of the sensors after name rewriting but prior to gui-url rewriting
        self.orig_sensors: aiokatcp.SensorSet
        try:
            self.orig_sensors = self.server.orig_sensors  # type: ignore
        except AttributeError:
            self.orig_sensors = aiokatcp.SensorSet()

        self.rewrite_gui_urls = rewrite_gui_urls
        if prometheus_labels is not None:
            self.prometheus_labels = prometheus_labels
        else:
            self.prometheus_labels = {}
        if prometheus_factory is not None:
            self.prometheus_factory = prometheus_factory
        else:
            self.prometheus_factory = _dummy_factory
        if prometheus_sensors is not None:
            self.prometheus_sensors = prometheus_sensors
        # Indexed by unqualified sensor name; None if no observer is needed.
        # Invariant: _observers has an entry if and only if the corresponding
        # sensor exists in self.server.sensors
        self._observers: Dict[str, Optional[PrometheusObserver]] = {}
        # Whether we need to do an interface-changed at the end of the batch
        self._interface_changed = False

    def rewrite_name(self, name: str) -> str:
        return self.prefix + name

    def _make_observer(self, name: str, sensor: aiokatcp.Sensor) -> Optional[PrometheusObserver]:
        """Make a :class:`PrometheusObserver` for client sensor `name`, if appropriate.
        Otherwise returns ``None``.
        """
        info = self.prometheus_factory(name, sensor)
        if info is None:
            return None
        try:
            value_metric, status_metric = self.prometheus_sensors[info.name]
        except KeyError:
            label_names = list(self.prometheus_labels.keys()) + list(info.labels.keys())
            value_metric = info.class_(info.name, info.description, label_names,
                                       registry=info.registry)
            status_metric = Gauge(info.name + '_status', f'Status of katcp sensor {info.name}',
                                  label_names, registry=info.registry)
            self.prometheus_sensors[info.name] = (value_metric, status_metric)

        label_values = list(self.prometheus_labels.values()) + list(info.labels.values())
        return PrometheusObserver(sensor, value_metric, status_metric, label_values)

    def sensor_added(self, name: str, description: str, units: str, type_name: str,
                     *args: bytes) -> None:
        """Add a new or replaced sensor with unqualified name `name`."""
        super().sensor_added(name, description, units, type_name, *args)
        sensor = self.sensors[self.rewrite_name(name)]
        self.orig_sensors.add(sensor)
        if (self.rewrite_gui_urls is not None
                and sensor.name.endswith('.gui-urls') and sensor.stype is bytes):
            new_value = self.rewrite_gui_urls(sensor)
            sensor = aiokatcp.Sensor(sensor.stype, sensor.name, sensor.description,
                                     sensor.units, new_value, sensor.status)
        self.server.sensors.add(sensor)
        old_observer = self._observers.get(name)
        if old_observer is not None:
            old_observer.close()
        self._observers[name] = self._make_observer(name, sensor)
        self._interface_changed = True

    def _sensor_removed(self, name: str) -> None:
        """Like :meth:`sensor_removed`, but takes the prefixed name"""
        self.server.sensors.pop(name, None)
        self.orig_sensors.pop(name, None)
        observer = self._observers.pop(name, None)
        if observer is not None:
            observer.close()
        self._interface_changed = True

    def sensor_removed(self, name: str) -> None:
        super().sensor_removed(name)
        rewritten = self.rewrite_name(name)
        self._sensor_removed(rewritten)

    def sensor_updated(self, name: str, value: bytes, status: aiokatcp.Sensor.Status,
                       timestamp: float) -> None:
        super().sensor_updated(name, value, status, timestamp)
        rewritten_name = self.rewrite_name(name)
        sensor = self.sensors[rewritten_name]
        if (self.rewrite_gui_urls is not None
                and rewritten_name.endswith('.gui-urls') and sensor.stype is bytes):
            value = self.rewrite_gui_urls(sensor)
            self.server.sensors[rewritten_name].set_value(value, status, timestamp)

    def batch_stop(self) -> None:
        super().batch_stop()
        if self._interface_changed:
            self.server.mass_inform('interface-changed', 'sensor-list')
        self._interface_changed = False

    def state_updated(self, state: aiokatcp.SyncState) -> None:
        super().state_updated(state)
        # TODO: move this into aiokatcp itself?
        if state == aiokatcp.SyncState.CLOSED:
            self.batch_start()
            for name in self.sensors.keys():
                self._sensor_removed(name)
            self.batch_stop()


class SensorProxyClient(aiokatcp.Client):
    """Client that mirrors sensors into a device server and Prometheus gauges.

    Parameters
    ----------
    server
        Server to which sensors will be added
    prefix
        String prepended to the remote server's sensor names to obtain names
        used on `server`. These should be unique per `server` to avoid
        collisions.
    prometheus_labels
        Extra labels to apply to every sensor.
    prometheus_factory
        Extracts information to create the Prometheus series from a sensor.
        It may return ``None`` to skip generating a Prometheus series for that
        sensor. This argument may also be ``None`` to skip creating any
        Prometheus metrics.
    prometheus_sensors
        Store for Prometheus metrics. If not provided, generated sensors are
        stored in a class-level variable. This is mainly intended to allow
        tests to be isolated from global state.
    rewrite_gui_urls
        If given, a function that is given a ``.gui-urls`` sensor and returns a
        replacement value. Note that the function is responsible for decoding
        and encoding between JSON and :class:`bytes`.
    args, kwargs
        Passed to the base class
    """

    def __init__(self, server: aiokatcp.DeviceServer, prefix: str,
                 prometheus_labels: Mapping[str, str] = None,
                 prometheus_factory: _Factory = None,
                 prometheus_sensors: Dict[str, Tuple[_LabelWrapper, _LabelWrapper]] = None,
                 rewrite_gui_urls: Callable[[aiokatcp.Sensor], bytes] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        watcher = SensorWatcher(self, server, prefix,
                                prometheus_labels, prometheus_factory, prometheus_sensors,
                                rewrite_gui_urls)
        self._synced = watcher.synced
        self.add_sensor_watcher(watcher)

    async def wait_synced(self) -> None:
        await self._synced.wait()
