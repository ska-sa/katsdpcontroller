"""Katcp device servers that emulate various container images."""

import json
import numbers
from typing import Dict, Optional, Tuple

import numpy as np
from aiokatcp import Sensor, Timestamp, FailReply
from .tasks import FakeDeviceServer


def _format_complex(value: numbers.Complex) -> str:
    """Format a complex number for a katcp request.

    This is copied from katgpucbf.
    """
    return f"{value.real}{value.imag:+}j"


class FakeFgpuDeviceServer(FakeDeviceServer):
    N_POLS = 2
    DEFAULT_GAIN = 1.0

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._gains = [np.full((1,), self.DEFAULT_GAIN, np.complex64) for _ in range(self.N_POLS)]
        sync_epoch_pos = self.logical_task.command.index("--sync-epoch")
        self._sync_epoch = float(self.logical_task.command[sync_epoch_pos + 1])
        self._adc_sample_rate = self.logical_task.streams[0].adc_sample_rate
        for pol in range(self.N_POLS):
            self.sensors.add(
                Sensor(
                    str,
                    f"input{pol}-eq",
                    "For this input, the complex, unitless, per-channel digital scaling factors "
                    "implemented prior to requantisation",
                    default="[1.0+0.0j]",
                    initial_status=Sensor.Status.NOMINAL
                )
            )
            self.sensors.add(
                Sensor(
                    str,
                    f"input{pol}-delay",
                    "The delay settings for this input: (loadmcnt <ADC sample "
                    "count when model was loaded>, delay <in seconds>, "
                    "delay-rate <unit-less or, seconds-per-second>, "
                    "phase <radians>, phase-rate <radians per second>).",
                    default="(-1, 0.0, 0.0, 0.0, 0.0)",
                    initial_status=Sensor.Status.NOMINAL
                )
            )

    async def request_delays(self, ctx, start_time: Timestamp, *delays: str) -> None:
        """Add a new first-order polynomial to the delay and fringe correction model."""
        # The real server only updates once the new model has gone into effect,
        # but since we're not simulating the data path we'll just update the
        # sensors immediately.
        assert len(delays) == self.N_POLS
        load_time = int((float(start_time) - self._sync_epoch) * self._adc_sample_rate)
        for i, delay_str in enumerate(delays):
            delay_args, phase_args = delay_str.split(':')
            delay, delay_rate = [float(x) for x in delay_args.split(',')]
            phase, phase_rate = [float(x) for x in phase_args.split(',')]
            value = f"({load_time}, {delay}, {delay_rate}, {phase}, {phase_rate})"
            self.sensors[f"input{i}-delay"].value = value

    async def request_gain(self, ctx, input: int, *values: str) -> Tuple[str, ...]:
        """Set or query the eq gains."""
        # Validation is handled by the subarray product, so we just trust here.
        if values:
            cvalues = np.array([np.complex64(v) for v in values])
            if np.all(cvalues == cvalues[0]):
                # Same value for all channels
                cvalues = cvalues[:1]
            self._gains[input] = cvalues
            self.sensors[f"input{input}-eq"].value = (
                "[" + ", ".join(_format_complex(gain) for gain in cvalues) + "]"
            )
        return tuple(_format_complex(v) for v in self._gains[input])

    async def request_gain_all(self, ctx, *values: str) -> None:
        """Set the eq gains for all inputs."""
        for pol in range(self.N_POLS):
            if values == ("default",):
                self._gains[pol] = np.full((1,), self.DEFAULT_GAIN, np.complex64)
            else:
                await self.request_gain(ctx, pol, *values)


class FakeIngestDeviceServer(FakeDeviceServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sensors.add(
            Sensor(bool, 'capture-active',
                   'Is there a currently active capture session (prometheus: gauge)',
                   default=False, initial_status=Sensor.Status.NOMINAL))

    async def request_capture_init(self, ctx, capture_block_id: str) -> None:
        """Dummy implementation of capture-init."""
        self.sensors['capture-active'].value = True

    async def request_capture_done(self, ctx) -> None:
        """Dummy implementation of capture-done."""
        self.sensors['capture-active'].value = False


class FakeCalDeviceServer(FakeDeviceServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._capture_blocks: Dict[str, str] = {}
        self._current_capture_block: Optional[str] = None
        self.sensors.add(
            Sensor(str, 'capture-block-state',
                   'JSON dict with the state of each capture block',
                   default='{}', initial_status=Sensor.Status.NOMINAL))

    def _update_capture_block_state(self) -> None:
        """Update the sensor from the internal state."""
        self.sensors['capture-block-state'].value = json.dumps(self._capture_blocks)

    async def request_capture_init(self, ctx, capture_block_id: str) -> None:
        """Add capture block ID to capture-block-state sensor."""
        if self._current_capture_block is not None:
            raise FailReply('A capture block is already active')
        self._current_capture_block = capture_block_id
        self._capture_blocks[capture_block_id] = 'CAPTURING'
        self._update_capture_block_state()

    async def request_capture_done(self, ctx) -> None:
        """Simulate the capture block going through all the states."""
        if self._current_capture_block is None:
            raise FailReply('Not currently capturing')
        cbid = self._current_capture_block
        self._current_capture_block = None
        self._capture_blocks[cbid] = 'PROCESSING'
        self._update_capture_block_state()
        self._capture_blocks[cbid] = 'REPORTING'
        self._update_capture_block_state()
        del self._capture_blocks[cbid]
        self._update_capture_block_state()
