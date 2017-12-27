"""Unit tests for :class:`katsdpcontroller.sensor_proxy_client`.

Still TODO:

- tests for Prometheus wrapping
- test that self.mirror.mass_inform is called
- test for the server removing a sensor before we can subscribe to it
- test for cancellation of the update in various cases
"""

import asyncio
import enum
from unittest import mock

import aiokatcp
from aiokatcp import Sensor, Address
from aiokatcp.test.test_utils import timelimit
import prometheus_client

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
        self.sensors.add(Sensor(str, 'str-sensor', 'String sensor',
                                default='hello', initial_status=Sensor.Status.ERROR))
        self.sensors.add(Sensor(bytes, 'bytes-sensor', 'Raw bytes sensor'))
        self.sensors.add(Sensor(bool, 'bool-sensor', 'Boolean sensor'))
        self.sensors.add(Sensor(Address, 'address-sensor', 'Address sensor'))
        self.sensors.add(Sensor(MyEnum, 'enum-sensor', 'Enum sensor'))
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
    async def setUp(self):
        self.mirror = mock.create_autospec(aiokatcp.DeviceServer, instance=True)
        self.mirror.sensors = aiokatcp.SensorSet([])
        self.server = DummyServer('127.0.0.1', 0)
        await self.server.start()
        self.addCleanup(self.server.stop)
        port = self.server.server.sockets[0].getsockname()[1]
        self.client = SensorProxyClient(self.mirror, 'prefix-', {}, [], '127.0.0.1', port)
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

    async def test_set_value(self):
        observer = FutureObserver(self.loop)
        self.mirror.sensors['prefix-int-sensor'].attach(observer)
        self.server.sensors['int-sensor'].set_value(2, timestamp=123456790.0)
        await observer.future
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
