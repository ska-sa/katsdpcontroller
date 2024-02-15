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

"""Tests for :mod:`katsdpcontroller.aggregate_sensors`."""

import re
from typing import List
from unittest import mock

import pytest
from aiokatcp import Reading, Sensor, SensorSet

from katsdpcontroller.aggregate_sensors import LatestSensor


@pytest.fixture
def target() -> SensorSet:
    return SensorSet()


class TestLatestSensor:
    """Test :class:`.LatestSensor`."""

    @pytest.fixture
    def int_sensor(self, target: SensorSet) -> LatestSensor[int]:
        return LatestSensor(
            target, int, "int-sensor", "int sensor", "bits", name_regex=re.compile("int-child-.*")
        )

    @pytest.fixture
    def str_sensor(self, target: SensorSet) -> LatestSensor[str]:
        return LatestSensor(
            target, str, "str-sensor", "str sensor", "", name_regex=re.compile("str-child-.*")
        )

    @pytest.fixture
    def int_children(self) -> List[Sensor[int]]:
        sensors = [Sensor(int, f"int-child-{i}", "") for i in range(4)]
        sensors[0].set_value(3, timestamp=1234567890.0)
        sensors[1].set_value(4, timestamp=1234567891.0, status=Sensor.Status.WARN)
        sensors[2].set_value(5, timestamp=1234567892.0)
        sensors[3].set_value(6, timestamp=1234567893.0, status=Sensor.Status.UNREACHABLE)
        return sensors

    def test_initial_state(
        self, int_sensor: LatestSensor[int], str_sensor: LatestSensor[str]
    ) -> None:
        assert int_sensor.reading == Reading(mock.ANY, Sensor.Status.UNKNOWN, 0)
        assert str_sensor.reading == Reading(mock.ANY, Sensor.Status.UNKNOWN, "")

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
