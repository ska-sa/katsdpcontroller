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
    """Watches a sensor and mirrors updates into a Prometheus Gauge or Counter"""
    def __init__(self, sensor, value_metric, status_metric):
        self._sensor = sensor
        self._old_value = 0.0
        self._value_metric = value_metric
        self._status_metric = status_metric
        sensor.attach(self)

    def __call__(self, sensor, reading):
        valid = reading.status in VALID_STATUS
        value = float(reading.value) if valid else 0.0
        self._status_metric.set(reading.status.value)
        # Detecting the type of the metric is tricky, because Counter and
        # Gauge aren't actually classes (they're functions). So we have to
        # use introspection.
        if type(self._value_metric).__name__ == 'Gauge':
            if valid:
                self._value_metric.set(value)
        elif type(self._value_metric).__name__ == 'Counter':
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
        else:
            raise TypeError('Expected a Counter or Gauge')
        if valid:
            self._old_value = value

    def close(self):
        """Shut down observing"""
        self._sensor.detach(self)
        self._status_metric.set(aiokatcp.Sensor.Status.UNREACHABLE.value)


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
        status value.
    prometheus_labels : list
        Labels to apply to the metrics in `prometheus_sensors`.
    args, kwargs
        Passed to the base class
    """
    def __init__(self, server, prefix, prometheus_sensors, prometheus_labels, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = server
        self.prefix = prefix
        self.prometheus_sensors = prometheus_sensors
        self.prometheus_labels = prometheus_labels
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
        try:
            normalised_name = name.replace(".", "_").replace("-", "_")
            value_metric, status_metric = self.prometheus_sensors[normalised_name]
            # It's tempting to push this work onto the caller, but that
            # causes the metric to be instantiated with the labels
            # whether the matching sensor exists or not.
            value_metric = value_metric.labels(*self.prometheus_labels)
            status_metric = status_metric.labels(*self.prometheus_labels)
        except KeyError:
            return None
        else:
            return PrometheusObserver(sensor, value_metric, status_metric)

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

    def _update_done(self, future):
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
