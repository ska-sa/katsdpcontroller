################################################################################
# Copyright (c) 2013-2024, National Research Foundation (SARAO)
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

"""Katcp device servers that emulate various container images."""

import json
import numbers
from typing import Dict, Optional, Tuple

import numpy as np
from aiokatcp import ClockState, DeviceStatus, FailReply, Sensor, SensorSet, Timestamp

from .tasks import FakeDeviceServer


def _format_complex(value: numbers.Complex) -> str:
    """Format a complex number for a katcp request.

    This is copied from katgpucbf.
    """
    return f"{value.real}{value.imag:+}j"


def _parse_key_val(value: str) -> Dict[str, str]:
    """Turn ``"foo=a,bar=b"`` into ``{"foo": "a", "bar": b"}``."""
    return dict(tuple(kv.split("=", 1)) for kv in value.split(","))  # type: ignore[misc]


def _add_device_status_sensor(sensors: SensorSet) -> None:
    sensors.add(
        Sensor(
            DeviceStatus,
            "device-status",
            "Overall engine health",
            default=DeviceStatus.DEGRADED,
            initial_status=Sensor.Status.WARN,
        )
    )


def _add_rx_device_status_sensor(sensors: SensorSet, description: str) -> None:
    sensors.add(
        Sensor(
            DeviceStatus,
            "rx.device-status",
            description,
            default=DeviceStatus.DEGRADED,
            initial_status=Sensor.Status.WARN,
        )
    )


def _add_time_sync_sensors(sensors: SensorSet) -> None:
    sensors.add(
        Sensor(
            float,
            "time.esterror",
            "Estimated time synchronisation error",
            units="s",
            default=0.0,
            initial_status=Sensor.Status.NOMINAL,
        )
    )
    sensors.add(
        Sensor(
            float,
            "time.maxerror",
            "Upper bound on time synchronisation error",
            units="s",
            default=0.0,
            initial_status=Sensor.Status.NOMINAL,
        )
    )
    sensors.add(
        Sensor(
            ClockState,
            "time.state",
            "Kernel clock state",
            default=ClockState.OK,
            initial_status=Sensor.Status.NOMINAL,
        )
    )
    sensors.add(
        Sensor(
            bool,
            "time.synchronised",
            "Whether the host clock is synchronised within tolerances",
            default=True,
            initial_status=Sensor.Status.NOMINAL,
        )
    )


def _add_steady_state_timestamp_sensor(sensors: SensorSet) -> None:
    sensors.add(
        Sensor(
            int,
            "steady-state-timestamp",
            "Heaps with this timestamp or greater are guaranteed to "
            "reflect the effects of previous katcp requests.",
            default=0,
            initial_status=Sensor.Status.NOMINAL,
        )
    )


class FakeFgpuDeviceServer(FakeDeviceServer):
    N_POLS = 2
    DEFAULT_GAIN = 1.0

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._sync_time = self.get_command_argument(float, "--sync-time")
        self._adc_sample_rate = self.logical_task.streams[0].adc_sample_rate
        outputs = self.get_command_arguments(_parse_key_val, "--wideband")
        outputs += self.get_command_arguments(_parse_key_val, "--narrowband")
        self._output_names = [output["name"] for output in outputs]
        self._gains = {
            output: [np.full((1,), self.DEFAULT_GAIN, np.complex64) for _ in range(self.N_POLS)]
            for output in self._output_names
        }
        for pol in range(self.N_POLS):
            for output in self._output_names:
                self.sensors.add(
                    Sensor(
                        str,
                        f"{output}.input{pol}.eq",
                        "For this input, the complex, unitless, per-channel "
                        "digital scaling factors implemented prior to requantisation",
                        default="[1.0+0.0j]",
                        initial_status=Sensor.Status.NOMINAL,
                    )
                )
                self.sensors.add(
                    Sensor(
                        str,
                        f"{output}.input{pol}.delay",
                        "The delay settings for this input: (loadmcnt <ADC sample "
                        "count when model was loaded>, delay <in seconds>, "
                        "delay-rate <unit-less or, seconds-per-second>, "
                        "phase <radians>, phase-rate <radians per second>).",
                        default="(-1, 0.0, 0.0, 0.0, 0.0)",
                        initial_status=Sensor.Status.NOMINAL,
                    )
                )
                self.sensors.add(
                    Sensor(
                        int,
                        f"{output}.input{pol}.feng-clip-cnt",
                        "Number of output samples that are saturated",
                        default=0,
                        initial_status=Sensor.Status.NOMINAL,
                    )
                )
            self.sensors.add(
                Sensor(
                    int,
                    f"input{pol}.dig-clip-cnt",
                    "Number of digitiser samples that are saturated",
                    default=0,
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            self.sensors.add(
                Sensor(
                    float,
                    f"input{pol}.dig-rms-dbfs",
                    "Digitiser ADC average power",
                    units="dBFS",
                    default=-25.0,
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            self.sensors.add(
                Sensor(
                    int,
                    f"input{pol}.rx.timestamp",
                    "The timestamp (in samples) of the last chunk of data received "
                    "from the digitiser",
                    default=-1,
                    initial_status=Sensor.Status.ERROR,
                )
            )
            self.sensors.add(
                Sensor(
                    Timestamp,
                    f"input{pol}.rx.unixtime",
                    "The timestamp (in UNIX time) of the last chunk of data received "
                    "from the digitiser",
                    default=Timestamp(-1.0),
                    initial_status=Sensor.Status.ERROR,
                )
            )
            self.sensors.add(
                Sensor(
                    Timestamp,
                    f"input{pol}.rx.missing-unixtime",
                    "The timestamp (in UNIX time) when missing data was last detected",
                    default=Timestamp(-1.0),
                    initial_status=Sensor.Status.NOMINAL,
                )
            )

        _add_time_sync_sensors(self.sensors)
        _add_device_status_sensor(self.sensors)
        _add_rx_device_status_sensor(
            self.sensors, "The F-engine is receiving a good, clean digitiser stream"
        )
        _add_steady_state_timestamp_sensor(self.sensors)

    def _check_stream_name(self, stream_name: str) -> None:
        """Validate that a stream name matches one of the outputs.

        Raises
        ------
        FailReply
            If `stream_name` is invalid.
        """
        if stream_name not in self._output_names:
            raise FailReply(f"no output stream called {stream_name!r}")

    async def request_delays(
        self, ctx, stream_name: str, start_time: Timestamp, *delays: str
    ) -> None:
        """Add a new first-order polynomial to the delay and fringe correction model."""
        # The real server only updates once the new model has gone into effect,
        # but since we're not simulating the data path we'll just update the
        # sensors immediately.
        self._check_stream_name(stream_name)
        assert len(delays) == self.N_POLS
        load_time = int((float(start_time) - self._sync_time) * self._adc_sample_rate)
        for i, delay_str in enumerate(delays):
            delay_args, phase_args = delay_str.split(":")
            delay, delay_rate = (float(x) for x in delay_args.split(","))
            phase, phase_rate = (float(x) for x in phase_args.split(","))
            value = f"({load_time}, {delay}, {delay_rate}, {phase}, {phase_rate})"
            self.sensors[f"{stream_name}.input{i}.delay"].value = value

    async def request_gain(
        self, ctx, stream_name: str, input: int, *values: str
    ) -> Tuple[str, ...]:
        """Set or query the eq gains."""
        self._check_stream_name(stream_name)
        # Validation is handled by the subarray product, so we just trust here.
        if values:
            cvalues = np.array([np.complex64(v) for v in values])
            if np.all(cvalues == cvalues[0]):
                # Same value for all channels
                cvalues = cvalues[:1]
            self._gains[stream_name][input] = cvalues
            self.sensors[f"{stream_name}.input{input}.eq"].value = (
                "[" + ", ".join(_format_complex(gain) for gain in cvalues) + "]"
            )
            return ()
        else:
            return tuple(_format_complex(v) for v in self._gains[stream_name][input])

    async def request_gain_all(self, ctx, stream_name: str, *values: str) -> None:
        """Set the eq gains for all inputs."""
        self._check_stream_name(stream_name)
        for pol in range(self.N_POLS):
            if values == ("default",):
                self._gains[stream_name][pol] = np.full((1,), self.DEFAULT_GAIN, np.complex64)
            else:
                await self.request_gain(ctx, stream_name, pol, *values)


class FakeXbgpuDeviceServer(FakeDeviceServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        antennas = self.get_command_argument(int, "--array-size")
        channel_offset = self.get_command_argument(int, "--channel-offset-value")
        channels_per_substream = self.get_command_argument(int, "--channels-per-substream")
        beam_outputs = self.get_command_arguments(_parse_key_val, "--beam")
        beam_names = [beam["name"] for beam in beam_outputs]
        corrprod_outputs = self.get_command_arguments(_parse_key_val, "--corrprod")
        corrprod_names = [corrprod["name"] for corrprod in corrprod_outputs]

        for beam_name in beam_names:
            self.sensors.add(
                Sensor(
                    str,
                    f"{beam_name}.chan-range",
                    "The range of channels processed by this B-engine, inclusive",
                    default=f"({channel_offset},{channel_offset + channels_per_substream - 1})",
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            default_delays = (0,) + (0.0, 0.0) * antennas
            self.sensors.add(
                Sensor(
                    str,
                    f"{beam_name}.delay",
                    "The delay settings of the inputs for this beam. Each input has "
                    "a delay [s] and phase [rad]: (loadmcnt, delay0, phase0, delay1,"
                    "phase1, ...)",
                    default=str(default_delays),
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            self.sensors.add(
                Sensor(
                    float,
                    f"{beam_name}.quantiser-gain",
                    "Non-complex post-summation quantiser gain applied to this beam",
                    default=1.0,
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            self.sensors.add(
                Sensor(
                    str,
                    f"{beam_name}.weight",
                    "The summing weights applied to all the inputs of this beam",
                    default=str([1.0] * antennas),
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            self.sensors.add(
                Sensor(
                    int,
                    f"{beam_name}.beng-clip-cnt",
                    "Number of complex samples that saturated",
                    default=0,
                    initial_status=Sensor.Status.NOMINAL,
                )
            )

        for corrprod_name in corrprod_names:
            self.sensors.add(
                Sensor(
                    bool,
                    f"{corrprod_name}.rx.synchronised",
                    "For the latest accumulation, was data present from all F-Engines.",
                    default=True,
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            self.sensors.add(
                Sensor(
                    int,
                    f"{corrprod_name}.xeng-clip-cnt",
                    "Number of visibilities that saturated",
                    default=0,
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
            self.sensors.add(
                Sensor(
                    str,
                    f"{corrprod_name}.chan-range",
                    "The range of channels processed by this X-engine, inclusive",
                    default=f"({channel_offset},{channel_offset + channels_per_substream - 1})",
                    initial_status=Sensor.Status.NOMINAL,
                )
            )
        self.sensors.add(
            Sensor(
                int,
                "rx.timestamp",
                "The timestamp (in samples) of the last chunk of data received from an F-engine",
                default=-1,
                initial_status=Sensor.Status.ERROR,
            )
        )
        self.sensors.add(
            Sensor(
                Timestamp,
                "rx.unixtime",
                "The timestamp (in UNIX time) of the last chunk of data received "
                "from an F-engine",
                default=Timestamp(-1.0),
                initial_status=Sensor.Status.ERROR,
            )
        )
        self.sensors.add(
            Sensor(
                Timestamp,
                "rx.missing-unixtime",
                "The timestamp (in UNIX time) when missing data was last detected",
                default=Timestamp(-1.0),
                initial_status=Sensor.Status.NOMINAL,
            )
        )

        _add_time_sync_sensors(self.sensors)
        _add_device_status_sensor(self.sensors)
        _add_rx_device_status_sensor(
            self.sensors, "The XB-engine is receiving a good, clean F-engine stream"
        )
        _add_steady_state_timestamp_sensor(self.sensors)

    async def request_beam_weights(self, ctx, stream_name: str, *weights: float) -> None:
        """Set beam weights."""
        self.sensors[f"{stream_name}.weight"].value = repr(list(weights))

    async def request_beam_quant_gains(self, ctx, stream_name: str, value: float) -> None:
        """Set beam quantisation gains."""
        self.sensors[f"{stream_name}.quantiser-gain"].value = value

    async def request_beam_delays(self, ctx, stream_name: str, *coefficient_sets: str) -> None:
        """Set beam delays."""
        params: list = [12345678]  # Dummy loadmcnt; typing is to prevent list[int] being inferred
        for coefficient_set in coefficient_sets:
            parts = [float(x) for x in coefficient_set.split(":")]
            params.extend(parts)
        self.sensors[f"{stream_name}.delay"].value = repr(tuple(params))


class FakeIngestDeviceServer(FakeDeviceServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sensors.add(
            Sensor(
                bool,
                "capture-active",
                "Is there a currently active capture session (prometheus: gauge)",
                default=False,
                initial_status=Sensor.Status.NOMINAL,
            )
        )

    async def request_capture_init(self, ctx, capture_block_id: str) -> None:
        """Dummy implementation of capture-init."""
        self.sensors["capture-active"].value = True

    async def request_capture_done(self, ctx) -> None:
        """Dummy implementation of capture-done."""
        self.sensors["capture-active"].value = False


class FakeCalDeviceServer(FakeDeviceServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._capture_blocks: Dict[str, str] = {}
        self._current_capture_block: Optional[str] = None
        self.sensors.add(
            Sensor(
                str,
                "capture-block-state",
                "JSON dict with the state of each capture block",
                default="{}",
                initial_status=Sensor.Status.NOMINAL,
            )
        )

    def _update_capture_block_state(self) -> None:
        """Update the sensor from the internal state."""
        self.sensors["capture-block-state"].value = json.dumps(self._capture_blocks)

    async def request_capture_init(self, ctx, capture_block_id: str) -> None:
        """Add capture block ID to capture-block-state sensor."""
        if self._current_capture_block is not None:
            raise FailReply("A capture block is already active")
        self._current_capture_block = capture_block_id
        self._capture_blocks[capture_block_id] = "CAPTURING"
        self._update_capture_block_state()

    async def request_capture_done(self, ctx) -> None:
        """Simulate the capture block going through all the states."""
        if self._current_capture_block is None:
            raise FailReply("Not currently capturing")
        cbid = self._current_capture_block
        self._current_capture_block = None
        self._capture_blocks[cbid] = "PROCESSING"
        self._update_capture_block_state()
        self._capture_blocks[cbid] = "REPORTING"
        self._update_capture_block_state()
        del self._capture_blocks[cbid]
        self._update_capture_block_state()
