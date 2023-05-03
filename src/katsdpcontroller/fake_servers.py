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


class FakeFgpuDeviceServer(FakeDeviceServer):
    N_POLS = 2
    DEFAULT_GAIN = 1.0

    @staticmethod
    def _parse_key_val(value: str) -> Dict[str, str]:
        """Turn ``"foo=a,bar=b"`` into ``{"foo": "a", "bar": b"}``."""
        return dict(tuple(kv.split("=", 1)) for kv in value.split(","))  # type: ignore[misc]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._sync_epoch = self.get_command_argument(float, "--sync-epoch")
        self._adc_sample_rate = self.logical_task.streams[0].adc_sample_rate
        outputs = self.get_command_arguments(self._parse_key_val, "--wideband")
        outputs += self.get_command_arguments(self._parse_key_val, "--narrowband")
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
                        float,
                        f"{output}.input{pol}.dig-rms-dbfs",
                        "Digitiser ADC average power",
                        units="dBFS",
                        default=-25.0,
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
        load_time = int((float(start_time) - self._sync_epoch) * self._adc_sample_rate)
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
        channel_offset = self.get_command_argument(int, "--channel-offset-value")
        channels_per_substream = self.get_command_argument(int, "--channels-per-substream")
        self.sensors.add(
            Sensor(
                bool,
                "rx.synchronised",
                "For the latest accumulation, was data present from all F-Engines.",
                default=True,
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        self.sensors.add(
            Sensor(
                int,
                "xeng-clip-cnt",
                "Number of visibilities that saturated",
                default=0,
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        self.sensors.add(
            Sensor(
                str,
                "chan-range",
                "The range of channels processed by this XB-engine, inclusive",
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
