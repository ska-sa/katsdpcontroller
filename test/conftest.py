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

import async_solipsism
import pytest

from unittest.mock import MagicMock
from aioresponses import core as aioresponses_core

pytest.register_assert_rewrite("test.utils")

# Workaround for aioresponses incompatibility with aiohttp>=3.14
# aiohttp 3.14 made stream_writer a required kwarg of ClientResponse
# but aioresponses does not pass it. Remove once aioresponses ships a fix.
try:
    from aioresponses import core as aioresponses_core
    _orig_build_response = aioresponses_core.aioresponses._build_response

    def _patched_build_response(self, url, **kwargs):
        if 'stream_writer' not in kwargs:
            kwargs['stream_writer'] = MagicMock()
        return _orig_build_response(self, url, **kwargs)

    aioresponses_core.aioresponses._build_response = _patched_build_response
except Exception:
    pass


@pytest.fixture
def solipsism(monkeypatch: pytest.MonkeyPatch) -> async_solipsism.EventLoopPolicy:
    """Use async_solipsism's event loop for the tests.

    Wrap this in an `event_loop_policy` fixture for a test.
    """
    monkeypatch.setattr(
        "aiohappyeyeballs.start_connection", async_solipsism.aiohappyeyeballs_start_connection
    )
    return async_solipsism.EventLoopPolicy()
