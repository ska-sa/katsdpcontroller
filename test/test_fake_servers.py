################################################################################
# Copyright (c) 2026, National Research Foundation (SARAO)
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

"""Tests for the sensor updates in fake FGPU and XBGPU servers."""

import asyncio
from types import SimpleNamespace

import pytest

from katsdpcontroller.fake_servers import (
    FakeFgpuDeviceServer,
    FakeXbgpuDeviceServer,
    _PeriodicSensorUpdates,
)


def _sensor_names(sensors):
    return {sensor.name for sensor in sensors}


@pytest.fixture
def event_loop_policy(solipsism):
    return solipsism


@pytest.mark.asyncio
async def test_update_rates(monkeypatch):
    updated = []
    monkeypatch.setattr(
        "katsdpcontroller.fake_servers._update_sensor", lambda sensor: updated.append(sensor)
    )
    updater = object.__new__(_PeriodicSensorUpdates)
    one_hz = object()
    two_hz = object()
    updater._start_sensor_updates([one_hz], [two_hz])
    try:
        await asyncio.sleep(1.1)
        assert updated == [two_hz, two_hz, one_hz]
    finally:
        updater._sensor_update_task.cancel()
        try:
            await updater._sensor_update_task
        except asyncio.CancelledError:
            pass
    updated.clear()
    updater._start_sensor_updates([one_hz])
    try:
        await asyncio.sleep(1.1)
        assert updated == [one_hz]
    finally:
        updater._sensor_update_task.cancel()
        try:
            await updater._sensor_update_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_fgpu_update_selection(monkeypatch):
    selected = []
    monkeypatch.setattr(
        FakeFgpuDeviceServer,
        "_start_sensor_updates",
        lambda self, one_hz: selected.append(_sensor_names(one_hz)),
    )
    stream = "wide-antenna-channelised-voltage"
    renames = {}
    for pol, label in enumerate(("m025h", "m025v")):
        for name in ("dig-clip-cnt", "dig-rms-dbfs", "rx.timestamp", "rx.unixtime"):
            renames[f"input{pol}.{name}"] = [f"{stream}.{label}.{name}"]
        renames[f"{stream}.input{pol}.feng-clip-cnt"] = f"{stream}.{label}.feng-clip-cnt"
    task = SimpleNamespace(
        command=["--sync-time=0", f"--wideband=name={stream}"],
        streams=[SimpleNamespace(adc_sample_rate=1.0)],
        sensor_renames=renames,
    )
    server = FakeFgpuDeviceServer("127.0.0.1", 0, task)
    assert "input0.dig-clip-cnt" in server.sensors
    assert selected == [
        {
            f"{stream}.input0.feng-clip-cnt",
            f"{stream}.input1.feng-clip-cnt",
            "input0.dig-clip-cnt",
            "input0.dig-rms-dbfs",
            "input1.dig-rms-dbfs",
            "input0.rx.timestamp",
            "input1.rx.timestamp",
            "input0.rx.unixtime",
            "input1.rx.unixtime",
        }
    ]


@pytest.mark.asyncio
async def test_xbgpu_update_selection(monkeypatch):
    selected = []
    monkeypatch.setattr(
        FakeXbgpuDeviceServer,
        "_start_sensor_updates",
        lambda self, one_hz, two_hz: selected.append(
            (_sensor_names(one_hz), _sensor_names(two_hz))
        ),
    )
    corrprod = "wide-baseline-correlation-products"
    beam = "wide-tied-array-channelised-voltage-0h"
    task = SimpleNamespace(
        command=[
            "--array-size=2",
            "--channel-offset-value=0",
            "--channels-per-substream=16",
            f"--corrprod=name={corrprod}",
            f"--beam=name={beam}",
        ],
        sensor_renames={
            "rx.timestamp": [f"{corrprod}.0.rx.timestamp", f"{beam}.0.rx.timestamp"],
            "rx.unixtime": [f"{corrprod}.0.rx.unixtime", f"{beam}.0.rx.unixtime"],
            f"{beam}.tx.next-timestamp": f"{beam}.0.tx.next-timestamp",
            f"{beam}.beng-clip-cnt": [],
            f"{corrprod}.rx.synchronised": [],
        },
    )
    FakeXbgpuDeviceServer("127.0.0.1", 0, task)
    assert selected == [
        (
            {
                "rx.timestamp",
                "rx.unixtime",
                f"{beam}.beng-clip-cnt",
                f"{beam}.tx.next-timestamp",
            },
            {f"{corrprod}.rx.synchronised", f"{corrprod}.tx.next-timestamp"},
        )
    ]
