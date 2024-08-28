################################################################################
# Copyright (c) 2023-2024, National Research Foundation (SARAO)
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

import re
from typing import Any, Iterable, Optional, Tuple, Type, TypeVar

from aiokatcp import (
    AggregateSensor,
    Reading,
    Sensor,
    SensorSampler,
    SensorSet,
    SimpleAggregateSensor,
)

_T = TypeVar("_T")


class SumSensor(SimpleAggregateSensor[int]):
    """Aggregate which takes the sum of its children.

    It also tracks how many child readings are present, and sets the state to
    FAILURE if they aren't all present.
    """

    def __init__(
        self,
        target: SensorSet,
        name: str,
        description: str,
        units: str = "",
        *,
        auto_strategy: Optional[SensorSampler.Strategy] = None,
        auto_strategy_parameters: Iterable[Any] = (),
        name_regex: re.Pattern,
        n_children: int,
    ) -> None:
        self.name_regex = name_regex
        self.n_children = n_children
        self._total = 0
        self._known = 0
        super().__init__(
            target,
            int,
            name,
            description,
            units,
            auto_strategy=auto_strategy,
            auto_strategy_parameters=auto_strategy_parameters,
        )

    def filter_aggregate(self, sensor: Sensor) -> bool:
        return bool(self.name_regex.fullmatch(sensor.name))

    def aggregate_add(self, sensor: Sensor[_T], reading: Reading[_T]) -> bool:
        assert isinstance(reading.value, int)
        if reading.status.valid_value():
            self._total += reading.value
            self._known += 1
            return True
        return False

    def aggregate_remove(self, sensor: Sensor[_T], reading: Reading[_T]) -> bool:
        assert isinstance(reading.value, int)
        if reading.status.valid_value():
            self._total -= reading.value
            self._known -= 1
            return True
        return False

    def aggregate_compute(self) -> Tuple[Sensor.Status, int]:
        status = Sensor.Status.NOMINAL if self._known == self.n_children else Sensor.Status.FAILURE
        return (status, self._total)


class SyncSensor(SimpleAggregateSensor[bool]):
    """Aggregate which tracks whether its children are synchronised.

    In this case,
    - a 'synchronised' child has a reading of (True, NOMINAL), and
    - an 'unsynchronised' child has a reading of (False, ERROR).
    """

    def __init__(
        self,
        target: SensorSet,
        name: str,
        description: str,
        units: str = "",
        *,
        auto_strategy: Optional["SensorSampler.Strategy"] = None,
        auto_strategy_parameters: Iterable[Any] = (),
        name_regex: re.Pattern,
        n_children: int,
    ) -> None:
        self.name_regex = name_regex
        self.n_children = n_children
        self._total_in_sync = 0

        super().__init__(
            target,
            bool,
            name,
            description,
            units,
            auto_strategy=auto_strategy,
            auto_strategy_parameters=auto_strategy_parameters,
        )

    def filter_aggregate(self, sensor: Sensor) -> bool:
        return bool(self.name_regex.fullmatch(sensor.name))

    def aggregate_add(self, sensor: Sensor[_T], reading: Reading[_T]) -> bool:
        assert isinstance(reading.value, bool)
        if reading.status.valid_value():
            if reading.value:
                self._total_in_sync += 1
            return True
        return False

    def aggregate_remove(self, sensor: Sensor[_T], reading: Reading[_T]) -> bool:
        assert isinstance(reading.value, bool)
        if reading.status.valid_value():
            if reading.value:
                self._total_in_sync -= 1
            return True
        return False

    def aggregate_compute(self) -> Tuple[Sensor.Status, bool]:
        synchronised = self._total_in_sync == self.n_children
        status = Sensor.Status.NOMINAL if synchronised else Sensor.Status.ERROR
        return (status, synchronised)

    def update_aggregate(
        self,
        updated_sensor: Optional[Sensor[_T]],
        reading: Optional[Reading[_T]],
        old_reading: Optional[Reading[_T]],
    ) -> Optional[Reading[bool]]:
        updated_reading = super().update_aggregate(updated_sensor, reading, old_reading)
        # Suppress updates that only change the timestamp
        if (
            updated_reading is not None
            and updated_reading.value == self.value
            and updated_reading.status == self.status
        ):
            updated_reading = None
        return updated_reading


class LatestSensor(AggregateSensor[_T]):
    """Aggregate sensor which returns the reading with the latest timestamp.

    If the latest reading is later removed, because the sensor vanishes (or
    the reading is updated with an older timestamp), the value of the
    aggregate persists.
    """

    def __init__(
        self,
        target: SensorSet,
        sensor_type: Type[_T],
        name: str,
        description: str,
        units: str = "",
        *,
        auto_strategy: Optional["SensorSampler.Strategy"] = None,
        auto_strategy_parameters: Iterable[Any] = (),
        name_regex: re.Pattern,
    ) -> None:
        self.name_regex = name_regex
        super().__init__(
            target,
            sensor_type,
            name,
            description,
            units,
            auto_strategy=auto_strategy,
            auto_strategy_parameters=auto_strategy_parameters,
        )

    def filter_aggregate(self, sensor: Sensor[_T]) -> bool:
        return bool(self.name_regex.fullmatch(sensor.name))

    def update_aggregate(
        self,
        updated_sensor: Optional[Sensor],
        reading: Optional[Reading],
        old_reading: Optional[Reading],
    ) -> Optional[Reading[_T]]:
        if reading is None or not reading.status.valid_value():
            return None  # It's not valid
        if self.status.valid_value() and self.timestamp > reading.timestamp:
            return None  # It's older than what we already have
        if reading.value == self.value and reading.status == self.status:
            return None  # It's the same as what we already have
        return reading
