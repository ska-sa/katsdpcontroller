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

"""Tests for :mod:`katsdpcontroller.generator`."""

import json

import katsdptelstate.aio
import katsdptelstate.aio.memory
from aiokatcp import SensorSet

from katsdpcontroller import generator
from katsdpcontroller.product_config import Configuration

from . import fake_katportalclient
from .utils import CONFIG, VLBI_CBF_SENSOR_VALUES, add_vlbi_config


def _mock_vlbi_cbf_sensors(mocker) -> None:
    client = fake_katportalclient.KATPortalClient(
        components={"cbf": "cbf_1", "sub": "subarray_1"},
        sensors=VLBI_CBF_SENSOR_VALUES,
    )
    mocker.patch("katportalclient.KATPortalClient", return_value=client)


async def test_vlbi_recorder_uses_cbf_multicast_interface(mocker) -> None:
    config = json.loads(CONFIG)
    add_vlbi_config(config)
    _mock_vlbi_cbf_sensors(mocker)
    configuration = await Configuration.from_config(config)
    graph = generator.build_logical_graph(configuration, config, SensorSet())
    vlbi_node = next(node for node in graph if node.name == "vlbi.sdp_vdif")
    source_multicast = next(
        node for node in graph if node.name == "multicast.cbf_tied_array_resampled_voltage"
    )
    assert source_multicast.port_name == "vdif"
    edge = next(iter(graph.get_edge_data(vlbi_node, source_multicast).values()))
    assert edge["port"] == "vdif"
    assert vlbi_node.command[4] == "{endpoints[multicast.cbf_tied_array_resampled_voltage_vdif]}"
    request = vlbi_node.interfaces[0]
    assert request.network == "cbf"
    assert request.multicast_in == {"cbf_tied_array_resampled_voltage"}
    assert request.infiniband is True
    assert request.affinity is True
    command = vlbi_node.command[2]
    assert "export J5A_PROTOCOL=udps" in command
    assert 'export J5A_CBF_INTERFACE="{interfaces[cbf].name}"' in command


async def test_vlbimeta_uses_cal_vis_stream(mocker) -> None:
    config = json.loads(CONFIG)
    add_vlbi_config(config)
    _mock_vlbi_cbf_sensors(mocker)
    del config["outputs"]["continuum_image"]
    del config["outputs"]["spectral_image"]
    configuration = await Configuration.from_config(config)
    telstate = katsdptelstate.aio.TelescopeState(katsdptelstate.aio.memory.MemoryBackend())
    graph = await generator.build_postprocess_logical_graph(
        configuration,
        "1234567890",
        telstate,
        "telstate.invalid:31000",
    )
    vlbimeta_node = next(node for node in graph if node.name == "vlbimeta.sdp_vdif")
    assert vlbimeta_node.command[:7] == [
        "vlbimeta.py",
        "/var/kat/data",
        "1234567890",
        "sdp_vdif",
        "--dataset-stream-name",
        "sdp_l0",
        "--mode",
    ]
    assert vlbimeta_node.command[7] == "antab"
