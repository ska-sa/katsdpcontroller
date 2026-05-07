################################################################################
# Copyright (c) 2013-2025, National Research Foundation (SARAO)
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

import asyncio
from types import SimpleNamespace
from unittest import mock

from aiokatcp import Sensor
import katsdptelstate
import katsdptelstate.aio
import pytest

from katsdpcontroller.tasks import KatcpTransition, TelstateSensorHistoryObserver


class TestKatcpTransition:
    def test_format(self):
        orig = KatcpTransition("{noformat}", "pos {}", "named {named}", 5, timeout=10)
        new = orig.format(123, named="foo")
        assert new.name == "{noformat}"
        assert new.args == ("pos 123", "named foo", 5)
        assert new.timeout == 10

    def test_repr(self):
        assert repr(KatcpTransition("name", timeout=5)) == "KatcpTransition('name', timeout=5)"
        assert (
            repr(KatcpTransition("name", "arg1", True, timeout=10))
            == "KatcpTransition('name', 'arg1', True, timeout=10)"
        )


class TestTelstateSensorHistoryObserver:
    @pytest.mark.asyncio
    async def test_persist_to_active_capture_block(self) -> None:
        sensor = Sensor(
            float,
            "gpucbf_tied_array_resampled_voltage.x0.mean-power",
            default=0.0,
            initial_status=Sensor.Status.NOMINAL,
        )
        telstate = katsdptelstate.aio.TelescopeState()
        task = SimpleNamespace(
            subarray_product=SimpleNamespace(
                telstate=telstate,
                current_capture_block=None,
            ),
            _capture_blocks=set(),
            logger=mock.Mock(),
        )
        observer = TelstateSensorHistoryObserver(
            sensor, task, "gpucbf_tied_array_resampled_voltage.x0.mean-power"
        )
        await asyncio.sleep(0)
        task.subarray_product.current_capture_block = SimpleNamespace(name="cbid")
        sensor.set_value(12.5, timestamp=34567.0)
        await asyncio.sleep(0)
        history = await telstate.view("cbid").get_range(
            "gpucbf_tied_array_resampled_voltage.x0.mean-power"
        )
        assert history == [(12.5, 34567.0)]
        assert (
            await telstate.view("cbid").key_type(
                "gpucbf_tied_array_resampled_voltage.x0.mean-power"
            )
            == katsdptelstate.KeyType.MUTABLE
        )
        observer.close()
