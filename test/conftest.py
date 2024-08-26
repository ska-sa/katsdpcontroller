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

pytest.register_assert_rewrite("test.utils")


@pytest.fixture
def solipsism(monkeypatch: pytest.MonkeyPatch) -> async_solipsism.EventLoopPolicy:
    """Use async_solipsism's event loop for the tests.

    Wrap this in an `event_loop_policy` fixture for a test.
    """
    monkeypatch.setattr(
        "aiohappyeyeballs.start_connection", async_solipsism.aiohappyeyeballs_start_connection
    )
    return async_solipsism.EventLoopPolicy()
