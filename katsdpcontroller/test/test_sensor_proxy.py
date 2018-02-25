"""Unit tests for :class:`katsdpcontroller.sensor_proxy_client`.

Still TODO:

- tests for Prometheus wrapping
- test that self.mirror.mass_inform is called
- test for the server removing a sensor before we can subscribe to it
- test for cancellation of the update in various cases
"""

import enum
from unittest import mock

import aiokatcp
from aiokatcp import Sensor, Address
from aiokatcp.test.test_utils import timelimit
from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry

import asynctest

from katsdpcontroller.sensor_proxy import SensorProxyClient


class MyEnum(enum.Enum):
    YES = 1
    NO = 2
    FILE_NOT_FOUND = 3


class DummyServer(aiokatcp.DeviceServer):
    """Dummy server that provides a range of sensors"""

    VERSION = '1.0'
    BUILD_STATE = '1.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sensors.add(Sensor(int, 'int-sensor', 'Integer sensor', 'frogs'))
        self.sensors.add(Sensor(float, 'float-sensor', 'Float sensor',
                                default=3.0, initial_status=Sensor.Status.NOMINAL))
        self.sensors.add(Sensor(float, 'histogram-sensor', 'Float sensor used for histogram'))
        self.sensors.add(Sensor(str, 'str-sensor', 'String sensor',
                                default='hello', initial_status=Sensor.Status.ERROR))
        self.sensors.add(Sensor(bytes, 'bytes-sensor', 'Raw bytes sensor'))
        self.sensors.add(Sensor(bool, 'bool-sensor', 'Boolean sensor'))
        self.sensors.add(Sensor(Address, 'address-sensor', 'Address sensor'))
        self.sensors.add(Sensor(MyEnum, 'enum-sensor', 'Enum sensor'))
        self.sensors.add(Sensor(float, 'dynamic-sensor', 'Test for prometheus_factory'))
        self.sensors['enum-sensor'].set_value(MyEnum.NO, timestamp=123456789)

    def add_sensor(self, sensor):
        self.sensors.add(sensor)
        self.mass_inform('#interface-changed', 'sensor-list')

    def remove_sensor(self, sensor_name):
        del self.sensors[sensor_name]
        self.mass_inform('#interface-changed', 'sensor-list')


class FutureObserver:
    def __init__(self, loop):
        self.future = loop.create_future()

    def __call__(self, sensor, reading):
        self.future.set_result(reading)


@timelimit
class TestSensorProxyClient(asynctest.TestCase):
    def _make_prom_sensor(self, name, description, class_, *args, **kwargs):
        return (
            class_('test_' + name, description, ['label1'], *args, registry=self.registry, **kwargs),
            Gauge('test_' + name + '_status',
                  'Status of katcp sensor ' + name, ['label1'], registry=self.registry))

    async def setUp(self):
        # Create a custom registry, to avoid polluting the global one
        self.registry = CollectorRegistry()
        prom_sensors = {
            'int_sensor': self._make_prom_sensor('int_sensor', 'A counter', Counter),
            'float_sensor': self._make_prom_sensor('float_sensor', 'A gauge', Gauge),
            'histogram_sensor': self._make_prom_sensor('histogram_sensor', 'A histogram', Histogram,
                                                       buckets=(1, 10))
        }

        def prom_factory(name, sensor):
            if sensor.name == 'prefix-dynamic-sensor':
                self.assertEqual(name, 'dynamic_sensor')
                return self._make_prom_sensor('dynamic_sensor', sensor.description, Counter)
            return None, None

        self.mirror = mock.create_autospec(aiokatcp.DeviceServer, instance=True)
        self.mirror.sensors = aiokatcp.SensorSet([])
        self.server = DummyServer('127.0.0.1', 0)
        await self.server.start()
        self.addCleanup(self.server.stop)
        port = self.server.server.sockets[0].getsockname()[1]
        self.client = SensorProxyClient(self.mirror, 'prefix-',
                                        prom_sensors, ['labelvalue1'], prom_factory,
                                        '127.0.0.1', port)
        self.addCleanup(self.client.wait_closed)
        self.addCleanup(self.client.close)
        await self.client.wait_synced()

    def _check_sensors(self):
        """Compare the upstream sensors against the mirror"""
        for sensor in self.server.sensors.values():
            qualname = 'prefix-' + sensor.name
            self.assertIn(qualname, self.mirror.sensors)
            sensor2 = self.mirror.sensors[qualname]
            self.assertEqual(sensor.description, sensor2.description)
            self.assertEqual(sensor.type_name, sensor2.type_name)
            self.assertEqual(sensor.units, sensor2.units)
            # We compare the encoded values rather than the values themselves,
            # because discretes have some special magic and it's the encoded
            # value that matters.
            self.assertEqual(aiokatcp.encode(sensor.value), aiokatcp.encode(sensor2.value))
            self.assertEqual(sensor.timestamp, sensor2.timestamp)
            self.assertEqual(sensor.status, sensor2.status)
        # Check that we don't have any we shouldn't
        for sensor2 in self.mirror.sensors.values():
            self.assertTrue(sensor2.name.startswith('prefix-'))
            base_name = sensor2.name[7:]
            self.assertIn(base_name, self.server.sensors)

    async def test_init(self):
        self._check_sensors()

    async def _set(self, name, value, **kwargs):
        """Set a sensor on the server and wait for the mirror to observe it"""
        observer = FutureObserver(self.loop)
        self.mirror.sensors['prefix-' + name].attach(observer)
        self.server.sensors[name].set_value(value, **kwargs)
        await observer.future
        self.mirror.sensors['prefix-' + name].detach(observer)

    async def test_set_value(self):
        await self._set('int-sensor', 2, timestamp=123456790.0)
        self._check_sensors()

    async def test_add_sensor(self):
        self.server.sensors.add(Sensor(int, 'another', 'another sensor', '', 234))
        # Rather than having server send an interface-changed inform, we invoke
        # it directly on the client so that we don't need to worry about timing.
        self.client.inform_interface_changed(b'sensor-list')
        await self.client.wait_synced()
        self._check_sensors()

    async def test_remove_sensor(self):
        del self.server.sensors['int-sensor']
        self.client.inform_interface_changed(b'sensor-list')
        await self.client.wait_synced()
        self._check_sensors()

    async def test_replace_sensor(self):
        self.server.sensors.add(Sensor(bool, 'int-sensor', 'Replaced by bool'))
        self.client.inform_interface_changed(b'sensor-list')
        await self.client.wait_synced()
        self._check_sensors()

    async def test_reconnect(self):
        # Cheat: the client will disconnect if given a #disconnect inform, and
        # we don't actually need to kill the server.
        self.client.inform_disconnect('Test')
        await self.client.wait_disconnected()
        await self.client.wait_synced()
        self._check_sensors()

    def _check_prom(self, name, value, status=Sensor.Status.NOMINAL, suffix='', extra_labels=None):
        labels = {'label1': 'labelvalue1'}
        value_labels = dict(labels)
        if extra_labels is not None:
            value_labels.update(extra_labels)
        actual_value = self.registry.get_sample_value(name + suffix, value_labels)
        actual_status = self.registry.get_sample_value(name + '_status', labels)
        self.assertEqual(value, actual_value)
        self.assertEqual(status.value, actual_status)

    async def test_gauge(self):
        await self._set('float-sensor', 2.5)
        self._check_prom('test_float_sensor', 2.5)
        # Change to an status where the value is not valid. The prometheus
        # Gauge must not change.
        await self._set('float-sensor', 1.0, status=Sensor.Status.FAILURE)
        self._check_prom('test_float_sensor', 2.5, Sensor.Status.FAILURE)

    async def test_histogram(self):
        # Record some values, check the counts
        await self._set('histogram-sensor', 4.0)
        await self._set('histogram-sensor', 5.0)
        await self._set('histogram-sensor', 0.5)
        await self._set('histogram-sensor', 100.0, timestamp=12345)
        self._check_prom('test_histogram_sensor', 1, suffix='_bucket', extra_labels={'le': '1.0'})
        self._check_prom('test_histogram_sensor', 3, suffix='_bucket', extra_labels={'le': '10.0'})
        self._check_prom('test_histogram_sensor', 4, suffix='_bucket', extra_labels={'le': '+Inf'})
        # Set same value and timestamp (spurious update)
        await self._set('histogram-sensor', 100.0, timestamp=12345)
        self._check_prom('test_histogram_sensor', 4, suffix='_bucket', extra_labels={'le': '+Inf'})
        # Set invalid value
        await self._set('histogram-sensor', 6.0, status=Sensor.Status.FAILURE)
        self._check_prom('test_histogram_sensor', 4, Sensor.Status.FAILURE,
                         suffix='_bucket', extra_labels={'le': '+Inf'})

    async def test_counter(self):
        await self._set('int-sensor', 4)
        self._check_prom('test_int_sensor', 4)
        # Increase the value
        await self._set('int-sensor', 10)
        self._check_prom('test_int_sensor', 10)
        # Reset then increase the value. The counter must record the cumulative total
        await self._set('int-sensor', 0)
        await self._set('int-sensor', 6, status=Sensor.Status.ERROR)
        self._check_prom('test_int_sensor', 16, Sensor.Status.ERROR)
        # Set to an invalid status. The counter value must not be affected.
        await self._set('int-sensor', 9, status=Sensor.Status.FAILURE)
        self._check_prom('test_int_sensor', 16, Sensor.Status.FAILURE)
        # Set back to a valid status
        await self._set('int-sensor', 8)
        self._check_prom('test_int_sensor', 18)

    async def test_prometheus_factory(self):
        await self._set('dynamic-sensor', 3.5)
        self._check_prom('test_dynamic_sensor', 3.5)
