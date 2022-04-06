"""Katcp device servers that emulate various container images."""

import json
from typing import Dict, Optional

from aiokatcp import Sensor, FailReply
from .tasks import FakeDeviceServer


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
