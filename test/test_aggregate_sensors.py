################################################################################
# Copyright (c) 2024, National Research Foundation (SARAO)
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

"""Tests for :mod:`katsdpcontroller.aggregate_sensors`."""

import re
from typing import List
from unittest import mock

import pytest
from aiokatcp import Reading, Sensor, SensorSet

from katsdpcontroller.aggregate_sensors import LatestSensor, SumSensor, SyncSensor


@pytest.fixture
def target() -> SensorSet:
    sensors = SensorSet()
    # Add an unrelated sensor to verify name_regex behaviour
    sensors.add(Sensor(int, "ignore-me", "", initial_status=Sensor.Status.NOMINAL, default=123))
    return sensors


@pytest.fixture
def int_children() -> List[Sensor[int]]:
    sensors = [Sensor(int, f"int-child-{i}", "") for i in range(4)]
    # No status_func, so default status is NOMINAL
    sensors[0].set_value(3, timestamp=1234567890.0)
    sensors[1].set_value(4, timestamp=1234567891.0, status=Sensor.Status.WARN)
    sensors[2].set_value(5, timestamp=1234567892.0)
    sensors[3].set_value(6, timestamp=1234567893.0, status=Sensor.Status.UNREACHABLE)
    return sensors


class TestSumSensor:
    """Test :class:`.SumSensor`."""

    @pytest.fixture
    def sum_sensor(self, target: SensorSet) -> SumSensor:
        return SumSensor(
            target,
            "sum-sensor",
            "sum sensor",
            "",
            name_regex=re.compile("int-child-.*"),
            n_children=4,
        )

    def test_initial_state(self, sum_sensor: SumSensor) -> None:
        """Before the children are added, the sensor status is FAILURE."""
        assert sum_sensor.reading == Reading(mock.ANY, Sensor.Status.FAILURE, 0)

    def test_bad_reading(
        self, sum_sensor: SumSensor, target: SensorSet, int_children: List[Sensor[int]]
    ) -> None:
        """When a child has a reading without a valid value, the sensor status is FAILURE."""
        for sensor in int_children:
            target.add(sensor)
        assert sum_sensor.reading == Reading(mock.ANY, Sensor.Status.FAILURE, 12)

    def test_good(
        self, sum_sensor: SumSensor, target: SensorSet, int_children: List[Sensor[int]]
    ) -> None:
        """Test behaviour when all the child sensors are valid."""
        for sensor in int_children:
            target.add(sensor)
        int_children[3].value = 10
        assert sum_sensor.reading == Reading(mock.ANY, Sensor.Status.NOMINAL, 22)

    def test_remove(
        self, sum_sensor: SumSensor, target: SensorSet, int_children: List[Sensor[int]]
    ) -> None:
        """Removing a sensor changes the status back to FAILURE."""
        self.test_good(sum_sensor, target, int_children)
        target.remove(int_children[0])
        assert sum_sensor.reading == Reading(mock.ANY, Sensor.Status.FAILURE, 19)


class TestSyncSensor:
    @pytest.fixture
    def sync_sensor(self, target: SensorSet) -> SyncSensor:
        return SyncSensor(
            target,
            "sync-sensor",
            "sync sensor",
            name_regex=re.compile("sync-child-.*"),
            n_children=2,
        )

    @pytest.fixture
    def children(self) -> List[Sensor[bool]]:
        sensors = [Sensor(bool, f"sync-child-{i}", "") for i in range(2)]
        sensors[0].set_value(True, status=Sensor.Status.NOMINAL)
        sensors[1].set_value(False, status=Sensor.Status.ERROR)
        return sensors

    def test_initial_state(self, sync_sensor: SyncSensor) -> None:
        # Should be unsynchronised because children are missing
        assert sync_sensor.reading == Reading(mock.ANY, Sensor.Status.ERROR, False)

    def test_all_children(
        self, target: SensorSet, sync_sensor: SyncSensor, children: List[Sensor[bool]]
    ) -> None:
        """Test with all child sensors present."""
        for sensor in children:
            target.add(sensor)
        assert sync_sensor.reading == Reading(mock.ANY, Sensor.Status.ERROR, False)
        children[1].set_value(True)
        assert sync_sensor.reading == Reading(mock.ANY, Sensor.Status.NOMINAL, True)
        children[0].set_value(False)
        assert sync_sensor.reading == Reading(mock.ANY, Sensor.Status.ERROR, False)
        children[0].set_value(True, status=Sensor.Status.FAILURE)  # Value should be ignored
        assert sync_sensor.reading == Reading(mock.ANY, Sensor.Status.ERROR, False)
        children[0].set_value(True)
        assert sync_sensor.reading == Reading(mock.ANY, Sensor.Status.NOMINAL, True)


class TestLatestSensor:
    """Test :class:`.LatestSensor`."""

    @pytest.fixture
    def int_sensor(self, target: SensorSet) -> LatestSensor[int]:
        return LatestSensor(
            target, int, "int-sensor", "int sensor", "bits", name_regex=re.compile("int-child-.*")
        )

    def test_initial_state(self, int_sensor: LatestSensor[int]) -> None:
        assert int_sensor.reading == Reading(mock.ANY, Sensor.Status.UNKNOWN, 0)

    def test_add(
        self, target: SensorSet, int_sensor: LatestSensor[int], int_children: List[Sensor[int]]
    ) -> None:
        target.add(int_children[1])
        assert int_sensor.reading == int_children[1].reading
        target.add(int_children[0])  # Older, so shouldn't affect it
        assert int_sensor.reading == int_children[1].reading
        target.add(int_children[2])  # Newer
        assert int_sensor.reading == int_children[2].reading
        target.add(int_children[3])  # Newer, but not a valid value
        assert int_sensor.reading == int_children[2].reading

    def test_remove(
        self, target: SensorSet, int_sensor: LatestSensor[int], int_children: List[Sensor[int]]
    ) -> None:
        target.add(int_children[0])
        target.remove(int_children[0])
        assert int_sensor.reading == int_children[0].reading

    def test_update(
        self, target: SensorSet, int_sensor: LatestSensor[int], int_children: List[Sensor[int]]
    ) -> None:
        orig_reading = int_children[0].reading
        target.add(int_children[0])
        int_children[0].set_value(10, timestamp=123.0)  # Older, should not update
        assert int_sensor.reading == orig_reading
        int_children[0].set_value(20, timestamp=1234567899.0, status=Sensor.Status.UNREACHABLE)
        assert int_sensor.reading == orig_reading
        int_children[0].set_value(30, timestamp=1234567898.0, status=Sensor.Status.WARN)
        assert int_sensor.reading == Reading(1234567898.0, Sensor.Status.WARN, 30)
