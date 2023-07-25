################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Unit tests for :class:`katsdpcontroller.sensor_proxy_client`.

Still TODO:

- tests for Prometheus wrapping
- test that mirror.mass_inform is called
- test for the server removing a sensor before we can subscribe to it
- test for cancellation of the update in various cases
"""

import asyncio
import enum
import functools
from typing import Any, AsyncGenerator, Dict, Mapping, Optional
from unittest import mock

import aiokatcp
import pytest
from aiokatcp import Address, Sensor, SensorSet
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from katsdpcontroller.controller import DeviceStatus, device_server_sockname
from katsdpcontroller.sensor_proxy import (
    CloseAction,
    PrometheusInfo,
    PrometheusWatcher,
    SensorProxyClient,
)


class MyEnum(enum.Enum):
    YES = 1
    NO = 2
    FILE_NOT_FOUND = 3


def _add_sensors(sensors: SensorSet) -> None:
    sensors.add(Sensor(int, "int-sensor", "Integer sensor", "frogs"))
    sensors.add(
        Sensor(
            float, "float-sensor", "Float sensor", default=3.0, initial_status=Sensor.Status.NOMINAL
        )
    )
    sensors.add(Sensor(float, "histogram-sensor", "Float sensor used for histogram"))
    sensors.add(
        Sensor(
            str, "str-sensor", "String sensor", default="hello", initial_status=Sensor.Status.ERROR
        )
    )
    sensors.add(Sensor(bytes, "bytes-sensor", "Raw bytes sensor"))
    sensors.add(Sensor(bool, "bool-sensor", "Boolean sensor"))
    sensors.add(Sensor(Address, "address-sensor", "Address sensor"))
    sensors.add(Sensor(MyEnum, "enum-sensor", "Enum sensor"))
    sensors.add(Sensor(int, "broadcast-sensor", "Sensor that is mapped to multiple copies"))
    sensors.add(Sensor(DeviceStatus, "device-status", "Device status"))
    sensors["enum-sensor"].set_value(MyEnum.NO, timestamp=123456789)


class DummyServer(aiokatcp.DeviceServer):
    """Dummy server that provides a range of sensors"""

    VERSION = "1.0"
    BUILD_STATE = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        _add_sensors(self.sensors)

    def add_sensor(self, sensor: aiokatcp.Sensor) -> None:
        self.sensors.add(sensor)
        self.mass_inform("#interface-changed", "sensor-list")

    def remove_sensor(self, sensor_name: str) -> None:
        del self.sensors[sensor_name]
        self.mass_inform("#interface-changed", "sensor-list")


class FutureObserver:
    def __init__(self) -> None:
        self.future = asyncio.get_event_loop().create_future()

    def __call__(self, sensor: aiokatcp.Sensor, reading: aiokatcp.Reading) -> None:
        self.future.set_result(reading)


@pytest.mark.timeout(5)
class TestSensorProxyClient:
    @pytest.fixture
    def mirror(self, mocker) -> mock.MagicMock:
        mirror = mocker.create_autospec(aiokatcp.DeviceServer, instance=True)
        mirror.sensors = aiokatcp.SensorSet()
        return mirror

    @pytest.fixture
    async def server(self) -> AsyncGenerator[DummyServer, None]:
        server = DummyServer("127.0.0.1", 0)
        await server.start()
        yield server
        await server.stop()

    @pytest.fixture
    def close_action(self) -> CloseAction:
        return CloseAction.REMOVE

    @pytest.fixture(autouse=True)
    async def client(
        self, mirror, server: DummyServer, close_action: CloseAction
    ) -> AsyncGenerator[SensorProxyClient, None]:
        port = device_server_sockname(server)[1]
        client = SensorProxyClient(
            mirror,
            "prefix-",
            renames={
                "bytes-sensor": "custom-bytes-sensor",
                "broadcast-sensor": ["copy01-broadcast-sensor", "copy02-broadcast-sensor"],
            },
            close_action=close_action,
            host="127.0.0.1",
            port=port,
        )
        await client.wait_synced()
        yield client

        client.close()
        await client.wait_closed()

    def _check_sensors(self, mirror, server: DummyServer) -> None:
        """Compare the upstream sensors against the mirror"""
        for sensor in server.sensors.values():
            qualnames = ["prefix-" + sensor.name]
            if sensor.name == "bytes-sensor":
                qualnames = ["custom-bytes-sensor"]
            elif sensor.name == "broadcast-sensor":
                qualnames = ["copy01-broadcast-sensor", "copy02-broadcast-sensor"]
            for qualname in qualnames:
                assert qualname in mirror.sensors
                sensor2 = mirror.sensors[qualname]
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
        for sensor2 in mirror.sensors.values():
            assert sensor2.name.startswith("prefix-") or sensor2.name in {
                "custom-bytes-sensor",
                "copy01-broadcast-sensor",
                "copy02-broadcast-sensor",
            }
            base_name = sensor2.name[7:]
            assert base_name in server.sensors
        assert "prefix-bytes-sensor" not in mirror.sensors

    async def test_init(self, mirror, server: DummyServer) -> None:
        self._check_sensors(mirror, server)

    async def _set(self, mirror, server: DummyServer, name: str, value: Any, **kwargs) -> None:
        """Set a sensor on the server and wait for the mirror to observe it"""
        observer = FutureObserver()
        mirror.sensors["prefix-" + name].attach(observer)
        server.sensors[name].set_value(value, **kwargs)
        await observer.future
        mirror.sensors["prefix-" + name].detach(observer)

    async def test_set_value(self, mirror, server: DummyServer) -> None:
        await self._set(mirror, server, "int-sensor", 2, timestamp=123456790.0)
        self._check_sensors(mirror, server)

    async def test_add_sensor(self, mirror, server: DummyServer, client: SensorProxyClient) -> None:
        server.sensors.add(Sensor(int, "another", "another sensor", "", 234))
        # Rather than having server send an interface-changed inform, we invoke
        # it directly on the client so that we don't need to worry about timing.
        changed = aiokatcp.Message.inform("interface-changed", b"sensor-list")
        client.handle_inform(changed)
        await client.wait_synced()
        self._check_sensors(mirror, server)

    async def test_remove_sensor(
        self, mirror, server: DummyServer, client: SensorProxyClient
    ) -> None:
        del server.sensors["int-sensor"]
        changed = aiokatcp.Message.inform("interface-changed", b"sensor-list")
        client.handle_inform(changed)
        await client.wait_synced()
        self._check_sensors(mirror, server)

    async def test_replace_sensor(
        self, mirror, server: DummyServer, client: SensorProxyClient
    ) -> None:
        server.sensors.add(Sensor(bool, "int-sensor", "Replaced by bool"))
        changed = aiokatcp.Message.inform("interface-changed", b"sensor-list")
        client.handle_inform(changed)
        await client.wait_synced()
        self._check_sensors(mirror, server)

    async def test_reconnect(self, mirror, server: DummyServer, client: SensorProxyClient) -> None:
        # Cheat: the client will disconnect if given a #disconnect inform, and
        # we don't actually need to kill the server.
        client.inform_disconnect("Test")
        await client.wait_disconnected()
        await client.wait_synced()
        self._check_sensors(mirror, server)

    async def test_close_action_remove(self, client: SensorProxyClient, mirror) -> None:
        client.close()
        assert list(mirror.sensors) == []

    @pytest.mark.parametrize("close_action", [CloseAction.UNREACHABLE])
    async def test_close_action_unreachable(self, client: SensorProxyClient, mirror) -> None:
        client.close()
        assert mirror.sensors["custom-bytes-sensor"].status == Sensor.Status.UNREACHABLE
        assert mirror.sensors["prefix-device-status"].status == Sensor.Status.ERROR
        assert mirror.sensors["prefix-device-status"].value.value == b"fail"


class TestPrometheusWatcher:
    def setup_method(self) -> None:
        # Create a custom registry, to avoid polluting the global one
        self.registry = CollectorRegistry()
        # Custom metric cache, to avoid polluting the global one
        prom_metrics: Dict[str, Any] = {}

        def prom_factory(sensor: aiokatcp.Sensor) -> Optional[PrometheusInfo]:
            if sensor.name == "int-sensor":
                return PrometheusInfo(Counter, "test_int_sensor", "A counter", {}, self.registry)
            elif sensor.name == "float-sensor":
                return PrometheusInfo(Gauge, "test_float_sensor", "A gauge", {}, self.registry)
            elif sensor.name == "bool-sensor":
                return PrometheusInfo(
                    Gauge,
                    "test_bool_sensor",
                    "A boolean gauge with labels",
                    {"label2": "labelvalue2"},
                    self.registry,
                )
            elif sensor.name == "histogram-sensor":
                return PrometheusInfo(
                    functools.partial(Histogram, buckets=(1, 10)),
                    "test_histogram_sensor",
                    "A histogram",
                    {},
                    self.registry,
                )
            elif sensor.name == "enum-sensor":
                return PrometheusInfo(Gauge, "test_enum_sensor", "An enum gauge", {}, self.registry)
            elif sensor.name == "dynamic-sensor":
                return PrometheusInfo(
                    Gauge, "test_dynamic_sensor", "Dynamic sensor", {}, self.registry
                )
            else:
                return None

        self.sensors = SensorSet()
        _add_sensors(self.sensors)
        self.watcher = PrometheusWatcher(
            self.sensors, {"label1": "labelvalue1"}, prom_factory, prom_metrics
        )

    def _check_prom(
        self,
        name: str,
        value: Optional[float],
        status: Optional[Sensor.Status] = Sensor.Status.NOMINAL,
        suffix: str = "",
        extra_labels: Optional[Mapping[str, str]] = None,
        extra_value_labels: Optional[Mapping[str, str]] = None,
    ):
        labels = {"label1": "labelvalue1"}
        if extra_labels is not None:
            labels.update(extra_labels)
        value_labels = dict(labels)
        if extra_value_labels is not None:
            value_labels.update(extra_value_labels)
        actual_value = self.registry.get_sample_value(name + suffix, value_labels)
        actual_status = self.registry.get_sample_value(name + "_status", labels)
        assert actual_value == value
        assert actual_status == (status.value if status is not None else None)

    def test_gauge(self) -> None:
        self._check_prom("test_float_sensor", 3.0)
        self.sensors["float-sensor"].value = 2.5
        self._check_prom("test_float_sensor", 2.5)
        # Change to a status where the value is not valid. The Prometheus
        # Gauge must not change.
        self.sensors["float-sensor"].set_value(1.0, status=Sensor.Status.FAILURE)
        self._check_prom("test_float_sensor", 2.5, Sensor.Status.FAILURE)

    def test_enum_gauge(self) -> None:
        self.sensors["enum-sensor"].value = MyEnum.NO
        self._check_prom("test_enum_sensor", 1.0)

    def test_histogram(self) -> None:
        # Record some values, check the counts
        sensor = self.sensors["histogram-sensor"]
        sensor.value = 4.0
        sensor.value = 5.0
        sensor.value = 0.5
        sensor.set_value(100.0, timestamp=12345)
        self._check_prom(
            "test_histogram_sensor", 1, suffix="_bucket", extra_value_labels={"le": "1.0"}
        )
        self._check_prom(
            "test_histogram_sensor", 3, suffix="_bucket", extra_value_labels={"le": "10.0"}
        )
        self._check_prom(
            "test_histogram_sensor", 4, suffix="_bucket", extra_value_labels={"le": "+Inf"}
        )
        # Set same value and timestamp (spurious update)
        sensor.set_value(100.0, timestamp=12345)
        self._check_prom(
            "test_histogram_sensor", 4, suffix="_bucket", extra_value_labels={"le": "+Inf"}
        )
        # Set invalid value
        sensor.set_value(6.0, status=Sensor.Status.FAILURE)
        self._check_prom(
            "test_histogram_sensor",
            4,
            Sensor.Status.FAILURE,
            suffix="_bucket",
            extra_value_labels={"le": "+Inf"},
        )

    def test_counter(self) -> None:
        sensor = self.sensors["int-sensor"]
        sensor.value = 4
        self._check_prom("test_int_sensor", 4)
        # Increase the value
        sensor.value = 10
        self._check_prom("test_int_sensor", 10)
        # Reset then increase the value. The counter must record the cumulative total
        sensor.value = 0
        sensor.set_value(6, status=Sensor.Status.ERROR)
        self._check_prom("test_int_sensor", 16, Sensor.Status.ERROR)
        # Set to an invalid status. The counter value must not be affected.
        sensor.set_value(9, status=Sensor.Status.FAILURE)
        self._check_prom("test_int_sensor", 16, Sensor.Status.FAILURE)
        # Set back to a valid status
        sensor.value = 8
        self._check_prom("test_int_sensor", 18)

    def test_add_sensor(self) -> None:
        # A non-Prometheus sensor, just to check that this doesn't break anything
        self.sensors.add(Sensor(int, "another", "another sensor", ""))
        self.sensors.add(Sensor(float, "dynamic-sensor", "dynamic sensor", ""))
        self.sensors["dynamic-sensor"].value = 345.0
        self._check_prom("test_dynamic_sensor", 345.0)

    def test_remove_sensor(self) -> None:
        del self.sensors["int-sensor"]
        self._check_prom("test_int_sensor", None, status=None)

    def test_extra_labels(self) -> None:
        self.sensors["bool-sensor"].value = True
        self._check_prom("test_bool_sensor", 1, extra_labels={"label2": "labelvalue2"})

    def test_close(self) -> None:
        self.watcher.close()
        self._check_prom("test_int_sensor", None, status=None)
