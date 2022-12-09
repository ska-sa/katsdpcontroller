"""Class for katcp connections that proxies sensors into a server"""

import logging
import enum
from typing import List, Tuple, Dict, Callable, Type, Mapping, Iterable, Sequence, Union, Optional

import aiokatcp
import prometheus_client       # noqa: F401
from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry


logger = logging.getLogger(__name__)
_Metric = Union[Gauge, Counter, Histogram]
# Use a string so that code won't completely break if Prometheus internals change.
# The Union is required because mypy doesn't like pure strings being used indirectly.
_LabelWrapper = Union['prometheus_client.core._LabelWrapper']
_Factory = Callable[[aiokatcp.Sensor], Optional['PrometheusInfo']]


def _dummy_factory(sensor: aiokatcp.Sensor) -> None:
    return None


def _reading_to_float(reading: aiokatcp.Reading) -> float:
    value = reading.value
    if isinstance(value, enum.Enum):
        try:
            values = list(type(value))     # type: List[enum.Enum]
            idx = values.index(value)
        except ValueError:
            idx = -1
        return float(idx)
    else:
        return float(reading.value)


class SensorWatcher(aiokatcp.SensorWatcher):
    """Mirrors sensors from a client into a server.

    See :class:`SensorProxyClient` for an explanation of the parameters.
    """

    def __init__(self, client: aiokatcp.Client, server: aiokatcp.DeviceServer,
                 prefix: str,
                 rewrite_gui_urls: Callable[[aiokatcp.Sensor], bytes] = None,
                 enum_types: Sequence[Type[enum.Enum]] = (),
                 renames: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(client, enum_types)
        self.prefix = prefix
        self.renames = renames if renames is not None else {}
        self.server = server
        # We keep track of the sensors after name rewriting but prior to gui-url rewriting
        self.orig_sensors: aiokatcp.SensorSet
        try:
            self.orig_sensors = self.server.orig_sensors  # type: ignore
        except AttributeError:
            self.orig_sensors = aiokatcp.SensorSet()

        self.rewrite_gui_urls = rewrite_gui_urls
        # Whether we need to do an interface-changed at the end of the batch
        self._interface_changed = False

    def rewrite_name(self, name: str) -> str:
        return self.renames.get(name, self.prefix + name)

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
        self._interface_changed = True

    def _sensor_removed(self, name: str) -> None:
        """Like :meth:`sensor_removed`, but takes the prefixed name"""
        self.server.sensors.pop(name, None)
        self.orig_sensors.pop(name, None)
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
    """Client that mirrors sensors into a device server.

    Parameters
    ----------
    server
        Server to which sensors will be added
    prefix
        String prepended to the remote server's sensor names to obtain names
        used on `server`. These should be unique per `server` to avoid
        collisions.
    rewrite_gui_urls
        If given, a function that is given a ``.gui-urls`` sensor and returns a
        replacement value. Note that the function is responsible for decoding
        and encoding between JSON and :class:`bytes`.
    renames
        Mapping from the remote server's sensor names to sensor names for
        `server`. Sensors found in this mapping do not have `prefix` applied.
    kwargs
        Passed to the base class
    """

    def __init__(self, server: aiokatcp.DeviceServer, prefix: str,
                 rewrite_gui_urls: Callable[[aiokatcp.Sensor], bytes] = None,
                 enum_types: Sequence[Type[enum.Enum]] = (),
                 renames: Optional[Mapping[str, str]] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        watcher = SensorWatcher(self, server, prefix, rewrite_gui_urls, renames=renames)
        self._synced = watcher.synced
        self.add_sensor_watcher(watcher)

    async def wait_synced(self) -> None:
        await self._synced.wait()


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

    def __init__(self, class_: Callable[..., _LabelWrapper],
                 name: str, description: str,
                 labels: Mapping[str, str],
                 registry: CollectorRegistry = prometheus_client.REGISTRY) -> None:
        self.class_ = class_
        self.name = name
        self.description = description
        self.labels = dict(labels)
        self.registry = registry


class PrometheusObserver:
    """Watches a sensor and mirrors updates into a Prometheus Gauge, Counter or Histogram."""

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
        value = _reading_to_float(reading) if valid else 0.0
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
            raise TypeError(f'Expected a Counter, Gauge or Histogram, not {metric_type}')
        if valid:
            self._old_value = value
        self._old_timestamp = timestamp

    def close(self) -> None:
        """Shut down observing"""
        self._sensor.detach(self)
        self._status_metric_root.remove(*self._label_values)
        self._value_metric_root.remove(*self._label_values)


class PrometheusWatcher:
    """Mirror sensors from a :class:`aiokatcp.SensorSet` into Prometheus metrics.

    It automatically deals with additions and removals of sensors from the
    sensor set.

    Parameters
    ----------
    sensors
        Sensors to monitor.
    labels
        Extra labels to apply to every sensor.
    factory
        Extracts information to create the Prometheus series from a sensor.
        It may return ``None`` to skip generating a Prometheus series for that
        sensor. This argument may also be ``None`` to skip creating any
        Prometheus metrics.
    metrics
        Store for Prometheus metrics. If not provided, generated sensors are
        stored in a class-level variable. This is mainly intended to allow
        tests to be isolated from global state.
    """

    # Caches metrics by name. Each entry stores the primary metric
    # and the status metric.
    _metrics: Dict[str, Tuple[_LabelWrapper, _LabelWrapper]] = {}

    def __init__(self, sensors: aiokatcp.SensorSet,
                 labels: Mapping[str, str] = None,
                 factory: _Factory = None,
                 metrics: Dict[str, Tuple[_LabelWrapper, _LabelWrapper]] = None) -> None:
        self.sensors = sensors
        # Indexed by sensor name; None if no observer is needed.
        # Invariant: _observers has an entry if and only if the corresponding
        # sensor exists in self.sensors (except after `close`).
        self._observers: Dict[str, Optional[PrometheusObserver]] = {}
        if labels is not None:
            self._labels = labels
        else:
            self._labels = {}
        if factory is not None:
            self._factory = factory
        else:
            self._factory = _dummy_factory
        if metrics is not None:
            self._metrics = metrics
        for sensor in self.sensors.values():
            self._added(sensor)
        self.sensors.add_add_callback(self._added)
        self.sensors.add_remove_callback(self._removed)

    def _make_observer(self, sensor: aiokatcp.Sensor) -> Optional[PrometheusObserver]:
        """Make a :class:`PrometheusObserver` for sensor `sensor`, if appropriate.

        Otherwise returns ``None``.
        """
        info = self._factory(sensor)
        if info is None:
            return None
        try:
            value_metric, status_metric = self._metrics[info.name]
        except KeyError:
            label_names = list(self._labels.keys()) + list(info.labels.keys())
            value_metric = info.class_(info.name, info.description, label_names,
                                       registry=info.registry)
            status_metric = Gauge(info.name + '_status', f'Status of katcp sensor {info.name}',
                                  label_names, registry=info.registry)
            self._metrics[info.name] = (value_metric, status_metric)

        label_values = list(self._labels.values()) + list(info.labels.values())
        observer = PrometheusObserver(sensor, value_metric, status_metric, label_values)
        # Populate initial value
        observer(sensor, sensor.reading)
        return observer

    def _added(self, sensor: aiokatcp.Sensor) -> None:
        old_observer = self._observers.get(sensor.name)
        if old_observer is not None:
            old_observer.close()
        self._observers[sensor.name] = self._make_observer(sensor)

    def _removed(self, sensor: aiokatcp.Sensor) -> None:
        old_observer = self._observers.pop(sensor.name, None)
        if old_observer is not None:
            old_observer.close()

    def close(self) -> None:
        for observer in self._observers.values():
            if observer is not None:
                observer.close()
        self._observers = {}
        try:
            self.sensors.remove_remove_callback(self._removed)
        except ValueError:
            pass
        try:
            self.sensors.remove_add_callback(self._added)
        except ValueError:
            pass
