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

from katsdpcontroller.tasks import KatcpTransition


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
