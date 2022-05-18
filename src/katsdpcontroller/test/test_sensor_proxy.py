"""Unit tests for :class:`katsdpcontroller.sensor_proxy_client`.

Still TODO:

- tests for Prometheus wrapping
- test that self.mirror.mass_inform is called
- test for the server removing a sensor before we can subscribe to it
- test for cancellation of the update in various cases
"""

import enum
import asyncio
import functools
import unittest
from unittest import mock
from typing import Dict, Mapping, Any, Optional

import aiokatcp
from aiokatcp import Sensor, SensorSet, Address
from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry

import asynctest

from ..sensor_proxy import SensorProxyClient, PrometheusInfo, PrometheusWatcher
from ..controller import device_server_sockname
from .utils import timelimit


class MyEnum(enum.Enum):
    YES = 1
    NO = 2
    FILE_NOT_FOUND = 3


def _add_sensors(sensors: SensorSet) -> None:
    sensors.add(Sensor(int, 'int-sensor', 'Integer sensor', 'frogs'))
    sensors.add(Sensor(float, 'float-sensor', 'Float sensor',
                       default=3.0, initial_status=Sensor.Status.NOMINAL))
    sensors.add(Sensor(float, 'histogram-sensor', 'Float sensor used for histogram'))
    sensors.add(Sensor(str, 'str-sensor', 'String sensor',
                       default='hello', initial_status=Sensor.Status.ERROR))
    sensors.add(Sensor(bytes, 'bytes-sensor', 'Raw bytes sensor'))
    sensors.add(Sensor(bool, 'bool-sensor', 'Boolean sensor'))
    sensors.add(Sensor(Address, 'address-sensor', 'Address sensor'))
    sensors.add(Sensor(MyEnum, 'enum-sensor', 'Enum sensor'))
    sensors['enum-sensor'].set_value(MyEnum.NO, timestamp=123456789)


class DummyServer(aiokatcp.DeviceServer):
    """Dummy server that provides a range of sensors"""

    VERSION = '1.0'
    BUILD_STATE = '1.0'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        _add_sensors(self.sensors)

    def add_sensor(self, sensor: aiokatcp.Sensor) -> None:
        self.sensors.add(sensor)
        self.mass_inform('#interface-changed', 'sensor-list')

    def remove_sensor(self, sensor_name: str) -> None:
        del self.sensors[sensor_name]
        self.mass_inform('#interface-changed', 'sensor-list')


class FutureObserver:
    def __init__(self) -> None:
        self.future = asyncio.get_event_loop().create_future()

    def __call__(self, sensor: aiokatcp.Sensor, reading: aiokatcp.Reading) -> None:
        self.future.set_result(reading)


@timelimit
class TestSensorProxyClient(asynctest.TestCase):
    async def setUp(self) -> None:
        self.mirror = mock.create_autospec(aiokatcp.DeviceServer, instance=True)
        self.mirror.sensors = aiokatcp.SensorSet()
        self.server = DummyServer('127.0.0.1', 0)
        await self.server.start()
        self.addCleanup(self.server.stop)
        assert self.server.server is not None
        assert self.server.server.sockets is not None
        port = device_server_sockname(self.server)[1]
        self.client = SensorProxyClient(
            self.mirror, 'prefix-', renames={'bytes-sensor': 'custom-bytes-sensor'},
            host='127.0.0.1', port=port)
        self.addCleanup(self.client.wait_closed)
        self.addCleanup(self.client.close)
        await self.client.wait_synced()

    def _check_sensors(self) -> None:
        """Compare the upstream sensors against the mirror"""
        for sensor in self.server.sensors.values():
            qualname = 'prefix-' + sensor.name
            if sensor.name == 'bytes-sensor':
                qualname = 'custom-bytes-sensor'
            assert qualname in self.mirror.sensors
            sensor2 = self.mirror.sensors[qualname]
            assert sensor.description == sensor2.description
            assert sensor.type_name == sensor2.type_name
            assert sensor.units == sensor2.units
            # We compare the encoded values rather than the values themselves,
            # because discretes have some special magic and it's the encoded
            # value that matters.
            assert aiokatcp.encode(sensor.value) == aiokatcp.encode(sensor2.value)
            assert sensor.timestamp == sensor2.timestamp
            assert sensor.status == sensor2.status
        # Check that we don't have any we shouldn't
        for sensor2 in self.mirror.sensors.values():
            assert sensor2.name.startswith('prefix-') or sensor2.name == 'custom-bytes-sensor'
            base_name = sensor2.name[7:]
            assert base_name in self.server.sensors
        assert 'prefix-bytes-sensor' not in self.mirror.sensors

    async def test_init(self) -> None:
        self._check_sensors()

    async def _set(self, name: str, value: Any, **kwargs) -> None:
        """Set a sensor on the server and wait for the mirror to observe it"""
        observer = FutureObserver()
        self.mirror.sensors['prefix-' + name].attach(observer)
        self.server.sensors[name].set_value(value, **kwargs)
        await observer.future
        self.mirror.sensors['prefix-' + name].detach(observer)

    async def test_set_value(self) -> None:
        await self._set('int-sensor', 2, timestamp=123456790.0)
        self._check_sensors()

    async def test_add_sensor(self) -> None:
        self.server.sensors.add(Sensor(int, 'another', 'another sensor', '', 234))
        # Rather than having server send an interface-changed inform, we invoke
        # it directly on the client so that we don't need to worry about timing.
        changed = aiokatcp.Message.inform('interface-changed', b'sensor-list')
        self.client.handle_inform(changed)
        await self.client.wait_synced()
        self._check_sensors()

    async def test_remove_sensor(self) -> None:
        del self.server.sensors['int-sensor']
        changed = aiokatcp.Message.inform('interface-changed', b'sensor-list')
        self.client.handle_inform(changed)
        await self.client.wait_synced()
        self._check_sensors()

    async def test_replace_sensor(self) -> None:
        self.server.sensors.add(Sensor(bool, 'int-sensor', 'Replaced by bool'))
        changed = aiokatcp.Message.inform('interface-changed', b'sensor-list')
        self.client.handle_inform(changed)
        await self.client.wait_synced()
        self._check_sensors()

    async def test_reconnect(self) -> None:
        # Cheat: the client will disconnect if given a #disconnect inform, and
        # we don't actually need to kill the server.
        self.client.inform_disconnect('Test')
        await self.client.wait_disconnected()
        await self.client.wait_synced()
        self._check_sensors()


class TestPrometheusWatcher(unittest.TestCase):
    def setUp(self) -> None:
        # Create a custom registry, to avoid polluting the global one
        self.registry = CollectorRegistry()
        # Custom metric cache, to avoid polluting the global one
        prom_metrics: Dict[str, Any] = {}

        def prom_factory(sensor: aiokatcp.Sensor) -> Optional[PrometheusInfo]:
            if sensor.name == 'int-sensor':
                return PrometheusInfo(Counter, 'test_int_sensor', 'A counter', {}, self.registry)
            elif sensor.name == 'float-sensor':
                return PrometheusInfo(Gauge, 'test_float_sensor', 'A gauge', {}, self.registry)
            elif sensor.name == 'bool-sensor':
                return PrometheusInfo(Gauge, 'test_bool_sensor', 'A boolean gauge with labels',
                                      {'label2': 'labelvalue2'}, self.registry)
            elif sensor.name == 'histogram-sensor':
                return PrometheusInfo(functools.partial(Histogram, buckets=(1, 10)),
                                      'test_histogram_sensor', 'A histogram', {}, self.registry)
            elif sensor.name == 'enum-sensor':
                return PrometheusInfo(Gauge, 'test_enum_sensor', 'An enum gauge',
                                      {}, self.registry)
            elif sensor.name == 'dynamic-sensor':
                return PrometheusInfo(Gauge, 'test_dynamic_sensor', 'Dynamic sensor',
                                      {}, self.registry)
            else:
                return None

        self.sensors = SensorSet()
        _add_sensors(self.sensors)
        self.watcher = PrometheusWatcher(self.sensors, {'label1': 'labelvalue1'}, prom_factory,
                                         prom_metrics)

    def _check_prom(self, name: str, value: Optional[float],
                    status: Optional[Sensor.Status] = Sensor.Status.NOMINAL,
                    suffix: str = '',
                    extra_labels: Mapping[str, str] = None,
                    extra_value_labels: Mapping[str, str] = None):
        labels = {'label1': 'labelvalue1'}
        if extra_labels is not None:
            labels.update(extra_labels)
        value_labels = dict(labels)
        if extra_value_labels is not None:
            value_labels.update(extra_value_labels)
        actual_value = self.registry.get_sample_value(name + suffix, value_labels)
        actual_status = self.registry.get_sample_value(name + '_status', labels)
        assert actual_value == value
        assert actual_status == (status.value if status is not None else None)

    def test_gauge(self) -> None:
        self._check_prom('test_float_sensor', 3.0)
        self.sensors['float-sensor'].value = 2.5
        self._check_prom('test_float_sensor', 2.5)
        # Change to a status where the value is not valid. The Prometheus
        # Gauge must not change.
        self.sensors['float-sensor'].set_value(1.0, status=Sensor.Status.FAILURE)
        self._check_prom('test_float_sensor', 2.5, Sensor.Status.FAILURE)

    def test_enum_gauge(self) -> None:
        self.sensors['enum-sensor'].value = MyEnum.NO
        self._check_prom('test_enum_sensor', 1.0)

    def test_histogram(self) -> None:
        # Record some values, check the counts
        sensor = self.sensors['histogram-sensor']
        sensor.value = 4.0
        sensor.value = 5.0
        sensor.value = 0.5
        sensor.set_value(100.0, timestamp=12345)
        self._check_prom('test_histogram_sensor', 1,
                         suffix='_bucket', extra_value_labels={'le': '1.0'})
        self._check_prom('test_histogram_sensor', 3,
                         suffix='_bucket', extra_value_labels={'le': '10.0'})
        self._check_prom('test_histogram_sensor', 4,
                         suffix='_bucket', extra_value_labels={'le': '+Inf'})
        # Set same value and timestamp (spurious update)
        sensor.set_value(100.0, timestamp=12345)
        self._check_prom('test_histogram_sensor', 4,
                         suffix='_bucket', extra_value_labels={'le': '+Inf'})
        # Set invalid value
        sensor.set_value(6.0, status=Sensor.Status.FAILURE)
        self._check_prom('test_histogram_sensor', 4, Sensor.Status.FAILURE,
                         suffix='_bucket', extra_value_labels={'le': '+Inf'})

    def test_counter(self) -> None:
        sensor = self.sensors['int-sensor']
        sensor.value = 4
        self._check_prom('test_int_sensor', 4)
        # Increase the value
        sensor.value = 10
        self._check_prom('test_int_sensor', 10)
        # Reset then increase the value. The counter must record the cumulative total
        sensor.value = 0
        sensor.set_value(6, status=Sensor.Status.ERROR)
        self._check_prom('test_int_sensor', 16, Sensor.Status.ERROR)
        # Set to an invalid status. The counter value must not be affected.
        sensor.set_value(9, status=Sensor.Status.FAILURE)
        self._check_prom('test_int_sensor', 16, Sensor.Status.FAILURE)
        # Set back to a valid status
        sensor.value = 8
        self._check_prom('test_int_sensor', 18)

    def test_add_sensor(self) -> None:
        # A non-Prometheus sensor, just to check that this doesn't break anything
        self.sensors.add(Sensor(int, 'another', 'another sensor', ''))
        self.sensors.add(Sensor(float, 'dynamic-sensor', 'dynamic sensor', ''))
        self.sensors['dynamic-sensor'].value = 345.0
        self._check_prom('test_dynamic_sensor', 345.0)

    def test_remove_sensor(self) -> None:
        del self.sensors['int-sensor']
        self._check_prom('test_int_sensor', None, status=None)

    def test_extra_labels(self) -> None:
        self.sensors['bool-sensor'].value = True
        self._check_prom('test_bool_sensor', 1, extra_labels={'label2': 'labelvalue2'})

    def test_close(self) -> None:
        self.watcher.close()
        self._check_prom('test_int_sensor', None, status=None)
