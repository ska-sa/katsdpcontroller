"""Class for katcp connections that proxies sensors into a server"""

import logging
import enum
import asyncio

import aiokatcp


VALID_STATUS = [
    aiokatcp.Sensor.Status.NOMINAL,
    aiokatcp.Sensor.Status.WARN,
    aiokatcp.Sensor.Status.ERROR]
SENSOR_TYPES = {
    'integer': int,
    'float': float,
    'boolean': bool,
    'timestamp': aiokatcp.Timestamp,
    'discrete': None,    # Type is constructed dynamically
    'address': aiokatcp.Address,
    'string': bytes      # Allows passing through arbitrary values even if not UTF-8
}
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
        valid = reading.status in VALID_STATUS
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


class DiscreteMixin:
    @property
    def katcp_value(self):
        return self.value


class SensorProxyClient(aiokatcp.Client):
    """Client that mirrors sensors into a device server,
    and optionally into Prometheus gauges as well.

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
        self.server = server
        self.prefix = prefix
        self.prometheus_sensors = prometheus_sensors
        self.prometheus_labels = prometheus_labels
        if prometheus_factory is None:
            prometheus_factory = lambda name, sensor: (None, None)     # noqa: E731
        self.prometheus_factory = prometheus_factory
        self.add_connected_callback(self.__connected)
        self.add_disconnected_callback(self.__disconnected)
        # Indexed by unqualified sensor name; None if no observer is needed.
        # Invariant: _observers has an entry if and only if the corresponding
        # sensor exists in self.server.sensors
        self._observers = {}
        self._update_task = None
        # Cache of dynamically built enum types for discrete sensors. The key is
        # a tuple of names.
        self._enum_cache = {}
        self._sampling_set = set()   # Sensors whose sampling strategy has been set
        self._synced = asyncio.Event(loop=self.loop)

    def qualify_name(self, name):
        """Map a remote server sensor name to a local name"""
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

    def _make_type(self, type_name, parameters):
        """Get the sensor type for a given type name"""
        if type_name == 'discrete':
            values = tuple(parameters)
            if values in self._enum_cache:
                return self._enum_cache[values]
            else:
                # We need unique Python identifiers for each value, but simply
                # normalising names in some way doesn't guarantee that.
                # Instead, we use arbitrary numbering.
                enums = [('ENUM{}'.format(i), value) for i, value in enumerate(values)]
                stype = enum.Enum('discrete', enums, type=DiscreteMixin)
                self._enum_cache[values] = stype
                return stype
        else:
            return SENSOR_TYPES[type_name]

    @staticmethod
    def _update_done(future):
        try:
            future.result()
        except asyncio.CancelledError:
            pass
        except OSError as error:
            # Connection died before we finished. Log it, but no need for
            # a stack trace.
            logger.warning('Connection error in update task: %s', error)
        except Exception:
            logger.exception('Exception in update task')

    def _trigger_update(self):
        logger.debug('Sensor sync triggered')
        self._synced.clear()
        if self._update_task is not None:
            self._update_task.cancel()
            self._update_task = None
        self._update_task = self.loop.create_task(self._update())
        self._update_task.add_done_callback(self._update_done)

    def _add_sensor(self, name, sensor):
        """Add a new or replaced sensor with unqualified name `name`."""
        old_observer = self._observers.get(name)
        if old_observer is not None:
            old_observer.close()
        self._observers[name] = self._make_observer(name, sensor)
        self.server.sensors.add(sensor)
        self._sampling_set.discard(name)

    def _remove_sensor(self, name):
        del self.server.sensors[self.qualify_name(name)]
        observer = self._observers.pop(name, None)
        if observer is not None:
            observer.close()
        self._sampling_set.discard(name)

    async def _set_sampling(self, names):
        """Register sampling strategy with sensors in `names`"""
        coros = [self.request('sensor-sampling', name, 'auto') for name in names]
        results = await asyncio.gather(*coros, loop=self.loop, return_exceptions=True)
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                try:
                    raise result
                except (aiokatcp.FailReply, aiokatcp.InvalidReply) as error:
                    logger.warning('Failed to set strategy on %s: %s', name, error)
            else:
                self._sampling_set.add(name)

    async def _update(self):
        reply, informs = await self.request('sensor-list')
        sampling = []
        seen = set()
        changed = False
        try:
            # Enumerate all sensors and add new or changed ones
            for inform in informs:
                name, description, units, type_name = [aiokatcp.decode(str, inform.arguments[i])
                                                       for i in range(4)]
                if type_name not in SENSOR_TYPES:
                    logger.warning('Type %s is not recognised, skipping sensor %s',
                                   type_name, name)
                    continue
                full_name = self.qualify_name(name)
                stype = self._make_type(type_name, inform.arguments[4:])
                seen.add(name)
                # Check if it already exists
                old = self.server.sensors.get(full_name)
                if (old is None
                        or old.description != description
                        or old.units != units
                        or old.stype is not stype):
                    sensor = aiokatcp.Sensor(stype, full_name, description, units)
                    self._add_sensor(name, sensor)
                    changed = True
                if name not in self._sampling_set:
                    sampling.append(name)
            # Remove old sensors
            for name in list(self._observers.keys()):
                if name not in seen:
                    self._remove_sensor(name)
                    changed = True
            await self._set_sampling(sampling)
            self._synced.set()
        finally:
            if changed:
                self.server.mass_inform('interface-changed', 'sensor-list')

    def __connected(self):
        self._sampling_set.clear()
        self._trigger_update()

    def __disconnected(self):
        self._synced.clear()
        self._sampling_set.clear()
        if self._update_task is not None:
            self._update_task.cancel()
            self._update_task = None
        for name in self._observers:
            sensor = self.server.sensors[self.qualify_name(name)]
            sensor.set_value(sensor.value, status=aiokatcp.Sensor.Status.UNREACHABLE)

    def inform_interface_changed(self, *args) -> None:
        # This could eventually be smarter and consult the args
        self._trigger_update()

    def inform_sensor_status(self, timestamp: aiokatcp.Timestamp, n: int, *args) -> None:
        if len(args) != 3 * n:
            raise aiokatcp.FailReply('Incorrect number of arguments')
        for i in range(n):
            name = '<unknown>'
            try:
                name = aiokatcp.decode(str, args[3 * i])
                status = aiokatcp.decode(aiokatcp.Sensor.Status, args[3 * i + 1])
                try:
                    sensor = self.server.sensors[self.qualify_name(name)]
                except KeyError:
                    logger.warning('Received update for unknown sensor %s', name)
                    continue
                value = aiokatcp.decode(sensor.stype, args[3 * i + 2])
                sensor.set_value(value, status=status, timestamp=timestamp)
            except Exception:
                logger.warning('Failed to process #sensor-status for %s', name, exc_info=True)

    async def wait_synced(self):
        await self._synced.wait()

    def close(self):
        if self._update_task is not None:
            self._update_task.cancel()
            self._update_task = None
        self._synced.clear()
        if self._observers:
            self.server.mass_inform('interface-changed', 'sensor-list')
        for name in list(self._observers.keys()):
            self._remove_sensor(name)
        super().close()
