"""Tests for :mod:`katsdpcontroller.product_config`."""

import copy
import re
from typing import Any, Dict, List, Optional
from unittest import mock

import jsonschema
import katpoint
import katportalclient
import pytest
import yarl

from katsdpcontroller import defaults, product_config
from katsdpcontroller.product_config import (
    STREAM_CLASSES,
    AntennaChannelisedVoltageStream,
    BaselineCorrelationProductsStream,
    BeamformerEngineeringStream,
    BeamformerStream,
    CalStream,
    CamHttpStream,
    Configuration,
    ContinuumImageStream,
    DigBasebandVoltageStream,
    DigBasebandVoltageStreamBase,
    FlagsStream,
    GpucbfAntennaChannelisedVoltageStream,
    GpucbfBaselineCorrelationProductsStream,
    Options,
    ServiceOverride,
    SimAntennaChannelisedVoltageStream,
    SimBaselineCorrelationProductsStream,
    SimDigBasebandVoltageStream,
    SimTiedArrayChannelisedVoltageStream,
    Simulation,
    SpectralImageStream,
    TiedArrayChannelisedVoltageStream,
    VisStream,
)

from . import fake_katportalclient

_M000 = katpoint.Antenna(
    "m000, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -8.258 -207.289 1.2075 5874.184 5875.444, -0:00:39.7 0 -0:04:04.4 -0:04:53.0 0:00:57.8 -0:00:13.9 0:13:45.2 0:00:59.8, 1.14"  # noqa: E501
)
_M002 = katpoint.Antenna(
    "m002, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -32.1085 -224.2365 1.248 5871.207 5872.205, 0:40:20.2 0 -0:02:41.9 -0:03:46.8 0:00:09.4 -0:00:01.1 0:03:04.7, 1.14"  # noqa: E501
)


class TestRecursiveDiff:
    """Test :meth:`~katsdpcontroller.product_config._recursive_diff`."""

    def test_base_add(self) -> None:
        out = product_config._recursive_diff({"a": 1}, {"a": 1, "b": 2})
        assert out == "b added"

    def test_base_remove(self) -> None:
        out = product_config._recursive_diff({"a": 1, "b": 2}, {"a": 1})
        assert out == "b removed"

    def test_base_change(self) -> None:
        out = product_config._recursive_diff({"a": 1, "b": 2}, {"a": 1, "b": 3})
        assert out == "b changed from 2 to 3"

    def test_nested_add(self) -> None:
        out = product_config._recursive_diff({"x": {}}, {"x": {"a": 1}})
        assert out == "x.a added"

    def test_nested_remove(self) -> None:
        out = product_config._recursive_diff({"x": {"a": 1}}, {"x": {}})
        assert out == "x.a removed"

    def test_nested_change(self) -> None:
        out = product_config._recursive_diff({"x": {"a": 1, "b": 2}}, {"x": {"a": 1, "b": 3}})
        assert out == "x.b changed from 2 to 3"


class TestOverride:
    """Test :meth:`~katsdpcontroller.product_config.override`."""

    def test_add(self) -> None:
        out = product_config.override({"a": 1}, {"b": 2})
        assert out == {"a": 1, "b": 2}

    def test_remove(self) -> None:
        out = product_config.override({"a": 1, "b": 2}, {"b": None})
        assert out == {"a": 1}
        out = product_config.override(out, {"b": None})  # Already absent
        assert out == {"a": 1}

    def test_replace(self) -> None:
        out = product_config.override({"a": 1, "b": 2}, {"b": 3})
        assert out == {"a": 1, "b": 3}

    def test_recurse(self) -> None:
        orig = {"a": {"aa": 1, "ab": 2}, "b": {"ba": {"c": 10}, "bb": [5]}}
        override = {"a": {"aa": [], "ab": None, "ac": 3}, "b": {"bb": [1, 2]}}
        out = product_config.override(orig, override)
        assert out == {"a": {"aa": [], "ac": 3}, "b": {"ba": {"c": 10}, "bb": [1, 2]}}


class TestUrlNEndpoints:
    """Test :meth:`~katsdpcontroller.product_config._url_n_endpoints`."""

    def test_simple(self) -> None:
        assert product_config._url_n_endpoints("spead://239.1.2.3+7:7148") == 8
        assert product_config._url_n_endpoints("spead://239.1.2.3:7148") == 1

    def test_yarl_url(self) -> None:
        assert product_config._url_n_endpoints(yarl.URL("spead://239.1.2.3+7:7148")) == 8
        assert product_config._url_n_endpoints(yarl.URL("spead://239.1.2.3:7148")) == 1

    def test_not_spead(self) -> None:
        with pytest.raises(ValueError, match="non-spead URL http://239.1.2.3:7148"):
            product_config._url_n_endpoints("http://239.1.2.3:7148")

    def test_missing_part(self) -> None:
        with pytest.raises(ValueError, match="URL spead:/path has no host"):
            product_config._url_n_endpoints("spead:/path")
        with pytest.raises(ValueError, match="URL spead://239.1.2.3 has no port"):
            product_config._url_n_endpoints("spead://239.1.2.3")


class TestNormaliseOutputChannels:
    """Test :meth:`~katsdpcontroller.product_config.normalise_output_channels`."""

    def test_given(self) -> None:
        assert product_config._normalise_output_channels(100, (20, 80)) == (20, 80)

    def test_absent(self) -> None:
        assert product_config._normalise_output_channels(100, None) == (0, 100)

    def test_empty(self) -> None:
        with pytest.raises(ValueError, match=r"output_channels is empty \(50:50\)"):
            product_config._normalise_output_channels(100, (50, 50))
        with pytest.raises(ValueError, match=r"output_channels is empty \(60:50\)"):
            product_config._normalise_output_channels(100, (60, 50))

    def test_overflow(self) -> None:
        with pytest.raises(
            ValueError, match=r"output_channels \(0:101\) overflows valid range 0:100"
        ):
            product_config._normalise_output_channels(100, (0, 101))
        with pytest.raises(
            ValueError, match=r"output_channels \(-1:1\) overflows valid range 0:100"
        ):
            product_config._normalise_output_channels(100, (-1, 1))

    def test_align(self) -> None:
        assert product_config._normalise_output_channels(1000, (299, 301), 100) == (200, 400)
        assert product_config._normalise_output_channels(1000, (200, 400), 100) == (200, 400)

    def test_misalign(self) -> None:
        with pytest.raises(
            ValueError, match=r"n_chans \(789\) is not a multiple of required alignment \(100\)"
        ):
            product_config._normalise_output_channels(789, (100, 200), 100)


class TestServiceOverride:
    """Test :class:`~.ServiceOverride`."""

    def test_from_config(self) -> None:
        config = {
            "config": {"hello": "world"},
            "taskinfo": {"image": "test"},
            "host": "test.invalid",
        }
        override = ServiceOverride.from_config(config)
        assert override.config == config["config"]
        assert override.taskinfo == config["taskinfo"]
        assert override.host == config["host"]

    def test_defaults(self) -> None:
        override = ServiceOverride.from_config({})
        assert override.config == {}
        assert override.taskinfo == {}
        assert override.host is None


class TestOptions:
    """Test :class:`~.Options`."""

    def test_from_config(self) -> None:
        config = {
            "develop": True,
            "wrapper": "http://test.invalid/wrapper.sh",
            "service_overrides": {"service1": {"host": "testhost"}},
        }
        options = Options.from_config(config)
        assert options.develop == config["develop"]
        assert options.wrapper == config["wrapper"]
        assert list(options.service_overrides.keys()) == ["service1"]
        assert options.service_overrides["service1"].host == "testhost"

    def test_defaults(self) -> None:
        options = Options.from_config({})
        assert options.develop is False
        assert options.wrapper is None
        assert options.service_overrides == {}


class TestSimulation:
    """Test :class:`~.Simulation`."""

    def test_from_config(self) -> None:
        config = {
            "sources": [
                "PKS 1934-63, radec, 19:39:25.03, -63:42:45.7, (200.0 12000.0 -11.11 7.777 -1.231)",
                "PKS 0408-65, radec, 4:08:20.38, -65:45:09.1, (800.0 8400.0 -3.708 3.807 -0.7202)",
            ],
            "start_time": 1234567890.0,
            "clock_ratio": 2.0,
        }
        sim = Simulation.from_config(config)
        assert sim.start_time == 1234567890.0
        assert sim.clock_ratio == 2.0
        assert len(sim.sources) == 2
        assert sim.sources[0].name == "PKS 1934-63"
        assert sim.sources[1].name == "PKS 0408-65"

    def test_defaults(self) -> None:
        sim = Simulation.from_config({})
        assert sim.start_time is None
        assert sim.clock_ratio == 1.0
        assert sim.sources == []

    def test_invalid_source(self) -> None:
        with pytest.raises(ValueError, match="Invalid source 1: .* must have at least two fields"):
            Simulation.from_config({"sources": ["blah"]})


class TestCamHttpStream:
    """Test :class:`~.CamHttpStream`."""

    def test_from_config(self) -> None:
        config = {"type": "cam.http", "url": "http://test.invalid"}
        cam_http = CamHttpStream.from_config(Options(), "cam_data", config, [], {})
        assert cam_http.name == "cam_data"
        assert cam_http.src_streams == []
        assert cam_http.url == yarl.URL("http://test.invalid")


class TestDigBasebandVoltageStream:
    """Test :class:`~.DigBasebandVoltageStream`."""

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sim.dig.baseband_voltage",
            "url": "spead://239.0.0.0+7:7148",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "band": "l",
            "antenna": "m000",
        }

    def test_from_config(self, config: Dict[str, Any]) -> None:
        dig = DigBasebandVoltageStream.from_config(Options(), "m000h", config, [], {})
        assert dig.url == yarl.URL(config["url"])
        assert dig.adc_sample_rate == config["adc_sample_rate"]
        assert dig.centre_frequency == config["centre_frequency"]
        assert dig.band == config["band"]
        assert dig.antenna_name == config["antenna"]
        assert dig.bits_per_sample == 10
        assert dig.data_rate(1.0, 0) == 1712e7


class TestSimDigBasebandVoltageStream:
    """Test :class:`~.SimDigBasebandVoltageStream`."""

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sim.dig.baseband_voltage",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "band": "l",
            "antenna": _M000.description,
        }

    def test_from_config(self, config: Dict[str, Any]) -> None:
        dig = SimDigBasebandVoltageStream.from_config(Options(), "m000h", config, [], {})
        assert dig.adc_sample_rate == config["adc_sample_rate"]
        assert dig.centre_frequency == config["centre_frequency"]
        assert dig.band == config["band"]
        assert dig.antenna == _M000
        assert dig.antenna_name == "m000"
        assert dig.bits_per_sample == 10
        assert dig.data_rate(1.0, 0) == 1712e7
        assert dig.command_line_extra == []

    def test_bad_antenna_description(self, config: Dict[str, Any]) -> None:
        with pytest.raises(ValueError, match="Invalid antenna description 'bad antenna': "):
            config["antenna"] = "bad antenna"
            SimDigBasebandVoltageStream.from_config(Options(), "m000h", config, [], {})

    def test_command_line_extra(self, config: Dict[str, Any]) -> None:
        config["command_line_extra"] = ["--extra-arg"]
        dig = SimDigBasebandVoltageStream.from_config(Options(), "m000h", config, [], {})
        assert dig.command_line_extra == config["command_line_extra"]


class TestAntennaChannelisedVoltageStream:
    """Test :class:`~.AntennaChannelisedVoltageStream`."""

    def test_from_config(self) -> None:
        config = {
            "type": "cbf.antenna_channelised_voltage",
            "url": "spead://239.0.0.0+7:7148",
            "antennas": ["m000", "another_antenna"],
            "instrument_dev_name": "narrow1",
        }
        sensors = {
            "band": "l",
            "adc_sample_rate": 1712e6,
            "n_chans": 32768,
            "bandwidth": 107e6,
            "centre_frequency": 1284e6,
            "n_samples_between_spectra": 524288,
        }
        acv = AntennaChannelisedVoltageStream.from_config(
            Options(), "narrow1_acv", config, [], sensors
        )
        assert acv.name == "narrow1_acv"
        assert acv.src_streams == []
        assert acv.url == yarl.URL("spead://239.0.0.0+7:7148")
        assert acv.antennas == ["m000", "another_antenna"]
        assert acv.band == "l"
        assert acv.n_chans == 32768
        assert acv.bandwidth == 107e6
        assert acv.centre_frequency == 1284e6
        assert acv.adc_sample_rate == 1712e6
        assert acv.n_samples_between_spectra == 524288
        assert acv.instrument_dev_name == "narrow1"


def make_dig_baseband_voltage(name: str) -> DigBasebandVoltageStream:
    urls = {
        "m000h": "spead2://239.1.2.0+7:7148",
        "m000v": "spead2://239.1.2.8+7:7148",
        "m002h": "spead2://239.1.2.16+7:7148",
        "m002v": "spead2://239.1.2.24+7:7148",
    }
    return DigBasebandVoltageStream(
        name,
        [],
        url=yarl.URL(urls[name]),
        adc_sample_rate=1712000000.0,
        centre_frequency=1284000000.0,
        band="l",
        antenna_name=name[:-1],
    )


def make_sim_dig_baseband_voltage(name: str) -> SimDigBasebandVoltageStream:
    return SimDigBasebandVoltageStream(
        name,
        [],
        adc_sample_rate=1712000000.0,
        centre_frequency=1284000000.0,
        band="l",
        antenna=_M000 if name.startswith("m000") else _M002,
    )


class TestGpucbfAntennaChanneliseVoltageStream:
    """Test :class:`~.GpucbfAntennaChannelisedVoltageStream`."""

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "gpucbf.antenna_channelised_voltage",
            "src_streams": ["m000h", "m000v", "m002h", "m002v"],
            "n_chans": 4096,
        }

    @pytest.fixture
    def src_streams(self, config: Dict[str, Any]) -> List[DigBasebandVoltageStreamBase]:
        # Use a real digitiser for one of them, to test mixing
        return [
            (
                make_sim_dig_baseband_voltage(name)
                if name != "m002h"
                else make_dig_baseband_voltage(name)
            )
            for name in config["src_streams"]
        ]

    def test_from_config(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), "wide1_acv", config, src_streams, {}
        )
        assert acv.name == "wide1_acv"
        assert acv.antennas == ["m000", "m002"]
        assert acv.band == src_streams[0].band
        assert acv.n_chans == config["n_chans"]
        assert acv.bandwidth == src_streams[0].adc_sample_rate / 2
        assert acv.centre_frequency == src_streams[0].centre_frequency
        assert acv.adc_sample_rate == src_streams[0].adc_sample_rate
        assert acv.n_samples_between_spectra == 2 * config["n_chans"]
        assert acv.sources(0) == tuple(src_streams[0:2])
        assert acv.sources(1) == tuple(src_streams[2:4])
        assert acv.data_rate(1.0, 0) == 27392e6 * 2
        assert acv.input_labels == config["src_streams"]
        assert acv.w_cutoff == 1.0  # Default value
        assert acv.command_line_extra == []

    def test_n_chans_not_power_of_two(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        for n_chans in [0, 3, 17]:
            with pytest.raises(ValueError):
                config["n_chans"] = n_chans
                GpucbfAntennaChannelisedVoltageStream.from_config(
                    Options(), "wide1_acv", config, src_streams, {}
                )

    def test_too_few_channels(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        with pytest.raises(ValueError):
            config["n_chans"] = 2
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), "wide1_acv", config, src_streams, {}
            )

    def test_src_streams_odd(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        with pytest.raises(ValueError, match="does not have an even number of elements"):
            del config["src_streams"][-1]
            del src_streams[-1]
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), "wide1_acv", config, src_streams, {}
            )

    def test_band_mismatch(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        with pytest.raises(ValueError, match=r"Inconsistent bands \(both l and u\)"):
            src_streams[1].band = "u"
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), "wide1_acv", config, src_streams, {}
            )

    def test_adc_sample_rate_mismatch(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        with pytest.raises(
            ValueError, match=r"Inconsistent ADC sample rates \(both 1712000000\.0 and 1\.0\)"
        ):
            src_streams[1].adc_sample_rate = 1.0
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), "wide1_acv", config, src_streams, {}
            )

    def test_centre_frequency_mismatch(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        with pytest.raises(
            ValueError, match=r"Inconsistent centre frequencies \(both 1284000000\.0 and 1\.0\)"
        ):
            src_streams[-1].centre_frequency = 1.0
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), "wide1_acv", config, src_streams, {}
            )

    def test_input_labels(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        config["input_labels"] = ["m900h", "m900v", "m901h", "m901v"]
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), "wide1_acv", config, src_streams, {}
        )
        assert acv.input_labels == config["input_labels"]

    def test_bad_input_labels(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        config["input_labels"] = ["m900h"]
        with pytest.raises(ValueError, match="input_labels has 1 elements, expected 4"):
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), "wide1_acv", config, src_streams, {}
            )
        config["input_labels"] = ["m900h"] * 4
        with pytest.raises(ValueError, match="are not unique"):
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), "wide1_acv", config, src_streams, {}
            )

    def test_w_cutoff(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        config["w_cutoff"] = 0.9
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), "wide1_acv", config, src_streams, {}
        )
        assert acv.w_cutoff == 0.9

    def test_command_line_extra(
        self, config: Dict[str, Any], src_streams: List[DigBasebandVoltageStreamBase]
    ) -> None:
        config["command_line_extra"] = ["--extra-arg"]
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), "wide1_acv", config, src_streams, {}
        )
        assert acv.command_line_extra == config["command_line_extra"]


class TestSimAntennaChannelisedVoltageStream:
    """Test :class:`~.SimAntennaChannelisedVoltageStream`."""

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sim.cbf.antenna_channelised_voltage",
            "antennas": [_M000.description, _M002.description],
            "band": "l",
            "centre_frequency": 1284e6,
            "bandwidth": 107e6,
            "adc_sample_rate": 1712e6,
            "n_chans": 32768,
        }

    def test_from_config(self, config: Dict[str, Any]) -> None:
        acv = SimAntennaChannelisedVoltageStream.from_config(
            Options(), "narrow1_acv", config, [], {}
        )
        assert acv.name == "narrow1_acv"
        assert acv.src_streams == []
        assert acv.antennas == ["m000", "m002"]
        assert acv.antenna_objects == [_M000, _M002]
        assert acv.band == "l"
        assert acv.n_chans == 32768
        assert acv.bandwidth == 107e6
        assert acv.centre_frequency == 1284e6
        assert acv.adc_sample_rate == 1712e6
        assert acv.n_samples_between_spectra == 524288

    def test_bad_bandwidth_ratio(self, config: Dict[str, Any]) -> None:
        with pytest.raises(ValueError, match="not a multiple of bandwidth"):
            config["bandwidth"] = 108e6
            SimAntennaChannelisedVoltageStream.from_config(Options(), "narrow1_acv", config, [], {})

    def test_bad_antenna_description(self, config: Dict[str, Any]) -> None:
        with pytest.raises(ValueError, match="Invalid antenna description 'bad antenna': "):
            config["antennas"][0] = "bad antenna"
            SimAntennaChannelisedVoltageStream.from_config(Options(), "narrow1_acv", config, [], {})


def make_antenna_channelised_voltage(
    antennas=("m000", "another_antenna")
) -> AntennaChannelisedVoltageStream:
    return AntennaChannelisedVoltageStream(
        "narrow1_acv",
        [],
        url=yarl.URL("spead2://239.0.0.0+7:7148"),
        antennas=antennas,
        band="l",
        n_chans=32768,
        bandwidth=107e6,
        adc_sample_rate=1712e6,
        centre_frequency=1284e6,
        n_samples_between_spectra=524288,
        instrument_dev_name="narrow1",
    )


def make_sim_antenna_channelised_voltage() -> SimAntennaChannelisedVoltageStream:
    # Uses powers of two so that integration time can be computed exactly
    return SimAntennaChannelisedVoltageStream(
        "narrow1_acv",
        [],
        antennas=[_M000, _M002],
        band="l",
        centre_frequency=1284,
        bandwidth=256,
        adc_sample_rate=1024,
        n_chans=512,
    )


def make_gpucbf_antenna_channelised_voltage() -> GpucbfAntennaChannelisedVoltageStream:
    src_streams = [
        make_sim_dig_baseband_voltage(name) for name in ["m000h", "m000v", "m001h", "m001v"]
    ]
    return GpucbfAntennaChannelisedVoltageStream("wide1_acv", src_streams, n_chans=4096)


class TestBaselineCorrelationProductsStream:
    """Test :class:`~.BaselineCorrelationProductsStream`."""

    @pytest.fixture
    def acv(self) -> AntennaChannelisedVoltageStream:
        return make_antenna_channelised_voltage()

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "cbf.baseline_correlation_products",
            "src_streams": ["narrow1_acv"],
            "url": "spead://239.1.0.0+7:7148",
            "instrument_dev_name": "narrow2",
        }

    @pytest.fixture
    def sensors(self) -> Dict[str, Any]:
        return {
            "int_time": 0.5,
            "n_bls": 40,
            "xeng_out_bits_per_sample": 32,
            "n_chans_per_substream": 2048,
        }

    def test_from_config(
        self, acv: AntennaChannelisedVoltageStream, config: Dict[str, Any], sensors: Dict[str, Any]
    ) -> None:
        bcp = BaselineCorrelationProductsStream.from_config(
            Options(), "narrow2_bcp", config, [acv], sensors
        )
        assert bcp.name == "narrow2_bcp"
        assert bcp.src_streams == [acv]
        assert bcp.int_time == 0.5
        assert bcp.n_baselines == 40
        assert bcp.n_vis == 40 * 32768
        assert bcp.size == 40 * 32768 * 8
        assert bcp.antenna_channelised_voltage is acv
        assert bcp.antennas == ["m000", "another_antenna"]
        assert bcp.n_chans == 32768
        assert bcp.n_chans_per_endpoint == 4096
        assert bcp.n_substreams == 16
        assert bcp.n_antennas == 2
        assert bcp.bandwidth == 107e6
        assert bcp.centre_frequency == 1284e6
        assert bcp.adc_sample_rate == 1712e6
        assert bcp.n_samples_between_spectra == 524288
        assert bcp.data_rate(1.0, 0) == 40 * 32768 * 8 * 2 * 8

    def test_bad_endpoint_count(
        self, acv: AntennaChannelisedVoltageStream, config: Dict[str, Any], sensors: Dict[str, Any]
    ) -> None:
        config["url"] = "spead://239.1.0.0+8:7148"
        with pytest.raises(
            ValueError, match=r"n_chans \(32768\) is not a multiple of endpoints \(9\)"
        ):
            BaselineCorrelationProductsStream.from_config(
                Options(), "narrow2_bcp", config, [acv], sensors
            )

    def test_bad_substream_count(
        self, acv: AntennaChannelisedVoltageStream, config: Dict[str, Any], sensors: Dict[str, Any]
    ) -> None:
        config["url"] = "spead://239.1.0.0+255:7148"
        with pytest.raises(
            ValueError,
            match=re.escape(
                r"channels per endpoint (128) is not a multiple of channels per substream (2048)"
            ),
        ):
            BaselineCorrelationProductsStream.from_config(
                Options(), "narrow2_bcp", config, [acv], sensors
            )


class TestGpucbfBaselineCorrelationProductsStream:
    """Test :class:`~.GpucbfBaselineCorrelationProductsStream`.

    This is not as thorough as :class:`TestBaselineCorrelationProductsStream`
    because a lot of those tests are actually testing the common base class.
    """

    @pytest.fixture
    def acv(self) -> GpucbfAntennaChannelisedVoltageStream:
        return make_gpucbf_antenna_channelised_voltage()

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "gpucbf.baseline_correlation_products",
            "src_streams": "wide1_acv",
            "int_time": 0.5,
        }

    def test_from_config(
        self, acv: GpucbfAntennaChannelisedVoltageStream, config: Dict[str, Any]
    ) -> None:
        bcp = GpucbfBaselineCorrelationProductsStream.from_config(
            Options(), "wide2_bcp", config, [acv], {}
        )
        # Note: the test values will probably need to be updated as the
        # implementation evolves.
        assert bcp.n_chans_per_substream == 1024
        assert bcp.n_substreams == 4
        assert bcp.int_time == 104448 * 4096 / 856e6
        assert bcp.command_line_extra == []

    def test_command_line_extra(
        self, acv: GpucbfAntennaChannelisedVoltageStream, config: Dict[str, Any]
    ) -> None:
        config["command_line_extra"] = ["--extra-arg"]
        bcp = GpucbfBaselineCorrelationProductsStream.from_config(
            Options(), "wide2_bcp", config, [acv], {}
        )
        assert bcp.command_line_extra == config["command_line_extra"]


class TestSimBaselineCorrelationProductsStream:
    """Test :class:`~.SimBaselineCorrelationProductsStream`.

    This is not as thorough as :class:`TestBaselineCorrelationProductsStream`
    because a lot of those tests are actually testing the common base class.
    """

    @pytest.fixture
    def acv(self) -> SimAntennaChannelisedVoltageStream:
        return make_sim_antenna_channelised_voltage()

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sim.cbf.baseline_correlation_products",
            "src_streams": ["narrow1_acv"],
            "n_endpoints": 16,
            "int_time": 800,
            "n_chans_per_substream": 8,
        }

    def test_from_config(
        self, acv: SimAntennaChannelisedVoltageStream, config: Dict[str, Any]
    ) -> None:
        bcp = SimBaselineCorrelationProductsStream.from_config(
            Options(), "narrow2_bcp", config, [acv], {}
        )
        assert bcp.n_chans_per_substream == 8
        assert bcp.n_substreams == 64
        # Check that int_time is rounded to nearest multiple of 512
        assert bcp.int_time == 1024.0

    def test_defaults(
        self, acv: SimAntennaChannelisedVoltageStream, config: Dict[str, Any]
    ) -> None:
        del config["n_chans_per_substream"]
        bcp = SimBaselineCorrelationProductsStream.from_config(
            Options(), "narrow2_bcp", config, [acv], {}
        )
        assert bcp.n_chans_per_substream == 32
        assert bcp.n_substreams == 16


class TestTiedArrayChannelisedVoltageStream:
    """Test :class:`~.TiedArrayChannelisedVoltageStream`."""

    @pytest.fixture
    def acv(self) -> AntennaChannelisedVoltageStream:
        return make_antenna_channelised_voltage()

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "cbf.tied_array_channelised_voltage",
            "src_streams": ["narrow1_acv"],
            "url": "spead://239.2.0.0+255:7148",
            "instrument_dev_name": "beam",
        }

    @pytest.fixture
    def sensors(self) -> Dict[str, Any]:
        return {
            "beng_out_bits_per_sample": 16,
            "spectra_per_heap": 256,
            "n_chans_per_substream": 64,
        }

    def test_from_config(
        self,
        acv: AntennaChannelisedVoltageStream,
        config: Dict[str, Any],
        sensors: Dict[str, Any],
    ) -> None:
        tacv = TiedArrayChannelisedVoltageStream.from_config(
            Options(), "beam_0x", config, [acv], sensors
        )
        assert tacv.name == "beam_0x"
        assert tacv.bits_per_sample == 16
        assert tacv.n_chans_per_substream == 64
        assert tacv.spectra_per_heap == 256
        assert tacv.instrument_dev_name == "beam"
        assert tacv.size == 32768 * 256 * 2 * 2
        assert tacv.antenna_channelised_voltage is acv
        assert tacv.bandwidth == 107e6
        assert tacv.antennas == ["m000", "another_antenna"]
        assert round(abs(tacv.int_time * 1712e6 - 524288 * 256), 7) == 0


class TestSimTiedArrayChannelisedVoltageStream:
    """Test :class:`~.SimTiedArrayChannelisedVoltageStream`."""

    @pytest.fixture
    def acv(self) -> SimAntennaChannelisedVoltageStream:
        return make_sim_antenna_channelised_voltage()

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sim.cbf.tied_array_channelised_voltage",
            "src_streams": ["narrow1_acv"],
            "n_endpoints": 16,
            "spectra_per_heap": 256,
            "n_chans_per_substream": 4,
        }

    def test_from_config(
        self, acv: SimAntennaChannelisedVoltageStream, config: Dict[str, Any]
    ) -> None:
        tacv = SimTiedArrayChannelisedVoltageStream.from_config(
            Options(), "beam_0x", config, [acv], {}
        )
        assert tacv.name == "beam_0x"
        assert tacv.bits_per_sample == 8
        assert tacv.n_chans_per_substream == 4
        assert tacv.spectra_per_heap == 256
        assert tacv.size == 512 * 256 * 2
        assert tacv.antenna_channelised_voltage is acv
        assert tacv.bandwidth == 256
        assert tacv.antennas == ["m000", "m002"]
        assert tacv.int_time == 512.0

    def test_defaults(
        self, acv: SimAntennaChannelisedVoltageStream, config: Dict[str, Any]
    ) -> None:
        del config["spectra_per_heap"]
        del config["n_chans_per_substream"]
        tacv = SimTiedArrayChannelisedVoltageStream.from_config(
            Options(), "beam_0x", config, [acv], {}
        )
        assert tacv.spectra_per_heap == defaults.KATCBFSIM_SPECTRA_PER_HEAP
        assert tacv.n_chans_per_substream == 32


def make_baseline_correlation_products(
    antenna_channelised_voltage: Optional[AntennaChannelisedVoltageStream] = None,
) -> BaselineCorrelationProductsStream:
    if antenna_channelised_voltage is None:
        antenna_channelised_voltage = make_antenna_channelised_voltage()
    return BaselineCorrelationProductsStream(
        "narrow1_bcp",
        [antenna_channelised_voltage],
        url=yarl.URL("spead://239.2.0.0+63:7148"),
        int_time=0.5,
        n_chans_per_substream=512,
        n_baselines=40,
        bits_per_sample=32,
        instrument_dev_name="narrow1",
    )


def make_tied_array_channelised_voltage(
    antenna_channelised_voltage: Optional[AntennaChannelisedVoltageStream], name: str, url: yarl.URL
) -> TiedArrayChannelisedVoltageStream:
    if antenna_channelised_voltage is None:
        antenna_channelised_voltage = make_antenna_channelised_voltage()
    return TiedArrayChannelisedVoltageStream(
        name,
        [antenna_channelised_voltage],
        url=url,
        n_chans_per_substream=128,
        spectra_per_heap=256,
        bits_per_sample=8,
        instrument_dev_name="beam",
    )


class TestVisStream:
    """Test :class:`~.VisStream`."""

    @pytest.fixture
    def bcp(self) -> BaselineCorrelationProductsStream:
        return make_baseline_correlation_products()

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sdp.vis",
            "src_streams": ["narrow1_bcp"],
            "output_int_time": 1.1,
            "output_channels": [128, 4096],
            "continuum_factor": 2,
            "excise": False,
            "archive": False,
        }

    def test_from_config(
        self, bcp: BaselineCorrelationProductsStream, config: Dict[str, Any]
    ) -> None:
        vis = VisStream.from_config(Options(), "sdp_l0", config, [bcp], {})
        assert vis.int_time == 1.0  # Rounds to nearest multiple of CBF int_time
        assert vis.output_channels == (128, 4096)
        assert vis.continuum_factor == 2
        assert vis.excise is False
        assert vis.archive is False
        assert vis.n_servers == 4
        assert vis.baseline_correlation_products is bcp
        assert vis.n_chans == 1984
        assert vis.n_spectral_chans == 3968
        assert vis.n_spectral_vis == 3968 * 12
        assert vis.antennas == ["m000", "another_antenna"]
        assert vis.n_antennas == 2
        assert vis.n_pols == 2
        assert vis.n_baselines == 12
        assert vis.size == 1984 * (12 * 10 + 4)
        assert vis.flag_size == 1984 * 12
        assert vis.data_rate(1.0, 0) == vis.size / vis.int_time * 8
        assert vis.flag_data_rate(1.0, 0) == vis.flag_size / vis.int_time * 8

    def test_defaults(self, bcp: BaselineCorrelationProductsStream, config: Dict[str, Any]) -> None:
        del config["output_channels"]
        del config["excise"]
        vis = VisStream.from_config(Options(), "sdp_l0", config, [bcp], {})
        assert vis.output_channels == (0, 32768)
        assert vis.excise is True

    def test_bad_continuum_factor(
        self, bcp: BaselineCorrelationProductsStream, config: Dict[str, Any]
    ) -> None:
        n_servers = 4
        continuum_factor = 3
        alignment = n_servers * continuum_factor
        config["continuum_factor"] = continuum_factor
        with pytest.raises(
            ValueError,
            match=re.escape(
                rf"n_chans ({bcp.n_chans}) is not a multiple of required alignment ({alignment})"
            ),
        ):
            VisStream.from_config(Options(), "sdp_l0", config, [bcp], {})

    def test_misaligned_output_channels(
        self, bcp: BaselineCorrelationProductsStream, config: Dict[str, Any]
    ) -> None:
        config["continuum_factor"] = 2048
        vis = VisStream.from_config(Options(), "sdp_l0", config, [bcp], {})
        assert vis.output_channels == (0, 8192)

    def test_compatible(
        self, bcp: BaselineCorrelationProductsStream, config: Dict[str, Any]
    ) -> None:
        vis1 = VisStream.from_config(Options(), "sdp_l0", config, [bcp], {})
        config["continuum_factor"] = 1
        config["archive"] = True
        vis2 = VisStream.from_config(Options(), "sdp_l0", config, [bcp], {})
        del config["output_channels"]
        vis3 = VisStream.from_config(Options(), "sdp_l0", config, [bcp], {})
        assert vis1.compatible(vis1)
        assert vis1.compatible(vis2)
        assert not vis1.compatible(vis3)

    def test_develop(self, bcp: BaselineCorrelationProductsStream, config: Dict[str, Any]) -> None:
        options = Options(develop=True)
        vis = VisStream.from_config(options, "sdp_l0", config, [bcp], {})
        assert vis.n_servers == 2


class TestBeamformerStream:
    """Test :class:`~.BeamformerStream`."""

    @pytest.fixture
    def tacv(self) -> List[TiedArrayChannelisedVoltageStream]:
        acv = make_antenna_channelised_voltage()
        return [
            make_tied_array_channelised_voltage(
                acv, "beam_0x", yarl.URL("spead://239.10.0.0+255:7148")
            ),
            make_tied_array_channelised_voltage(
                acv, "beam_0y", yarl.URL("spead://239.10.1.0+255:7148")
            ),
        ]

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {"type": "sdp.beamformer", "src_streams": ["beam_0x", "beam_0y"]}

    def test_from_config(
        self, tacv: List[TiedArrayChannelisedVoltageStream], config: Dict[str, Any]
    ) -> None:
        bf = BeamformerStream.from_config(Options(), "beamformer", config, tacv, {})
        assert bf.name == "beamformer"
        assert bf.antenna_channelised_voltage.name == "narrow1_acv"
        assert bf.tied_array_channelised_voltage == tacv
        assert bf.n_chans == 32768

    def test_mismatched_sources(
        self, tacv: List[TiedArrayChannelisedVoltageStream], config: Dict[str, Any]
    ) -> None:
        acv = make_antenna_channelised_voltage(antennas=["m012", "s0013"])
        tacv[1] = make_tied_array_channelised_voltage(
            acv, "beam_0y", yarl.URL("spead://239.10.1.0+255:7148")
        )
        with pytest.raises(
            ValueError, match="Source streams do not come from the same channeliser"
        ):
            BeamformerStream.from_config(Options(), "beamformer", config, tacv, {})


class TestBeamformerEngineeringStream:
    # TODO: this is duplicated from TestBeamformerStream
    @pytest.fixture
    def tacv(self) -> List[TiedArrayChannelisedVoltageStream]:
        acv = make_antenna_channelised_voltage()
        return [
            make_tied_array_channelised_voltage(
                acv, "beam_0x", yarl.URL("spead://239.10.0.0+255:7148")
            ),
            make_tied_array_channelised_voltage(
                acv, "beam_0y", yarl.URL("spead://239.10.1.0+255:7148")
            ),
        ]

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sdp.beamformer_engineering",
            "src_streams": ["beam_0x", "beam_0y"],
            "output_channels": [128, 1024],
            "store": "ram",
        }

    def test_from_config(
        self, tacv: List[TiedArrayChannelisedVoltageStream], config: Dict[str, Any]
    ) -> None:
        bf = BeamformerEngineeringStream.from_config(Options(), "beamformer", config, tacv, {})
        assert bf.name == "beamformer"
        assert bf.antenna_channelised_voltage.name == "narrow1_acv"
        assert bf.tied_array_channelised_voltage == tacv
        assert bf.store == "ram"
        assert bf.output_channels == (128, 1024)
        assert bf.n_chans == 896

    def test_defaults(
        self, tacv: List[TiedArrayChannelisedVoltageStream], config: Dict[str, Any]
    ) -> None:
        del config["output_channels"]
        bf = BeamformerEngineeringStream.from_config(Options(), "beamformer", config, tacv, {})
        assert bf.output_channels == (0, 32768)
        assert bf.n_chans == 32768

    def test_misaligned_channels(
        self, tacv: List[TiedArrayChannelisedVoltageStream], config: Dict[str, Any]
    ) -> None:
        config["output_channels"] = [1, 2]
        bf = BeamformerEngineeringStream.from_config(Options(), "beamformer", config, tacv, {})
        assert bf.output_channels == (0, 128)


def make_vis(
    baseline_correlation_products: Optional[BaselineCorrelationProductsStream] = None,
) -> VisStream:
    if baseline_correlation_products is None:
        baseline_correlation_products = make_baseline_correlation_products()
    return VisStream(
        "sdp_l0",
        [baseline_correlation_products],
        int_time=8.0,
        continuum_factor=1,
        excise=True,
        archive=True,
        n_servers=4,
    )


def make_vis_4ant() -> VisStream:
    acv = make_antenna_channelised_voltage(["m000", "m001", "s0002", "another_antenna"])
    bcp = make_baseline_correlation_products(acv)
    return make_vis(bcp)


class TestCalStream:
    @pytest.fixture
    def vis(self) -> VisStream:
        # The default has only 2 antennas, but we need 4 to make it legal
        return make_vis_4ant()

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sdp.cal",
            "src_streams": ["sdp_l0"],
            "parameters": {"g_solint": 2.0},
            "buffer_time": 600.0,
            "max_scans": 100,
        }

    def test_from_config(self, vis: VisStream, config: Dict[str, Any]) -> None:
        cal = CalStream.from_config(Options(), "cal", config, [vis], {})
        assert cal.name == "cal"
        assert cal.vis is vis
        # Make sure it's not referencing the original
        config["parameters"].clear()
        assert cal.parameters == {"g_solint": 2.0}
        assert cal.buffer_time == 600.0
        assert cal.max_scans == 100
        assert cal.n_antennas == 4
        assert cal.slots == 75

    def test_defaults(self, vis: VisStream, config: Dict[str, Any]) -> None:
        del config["parameters"]
        del config["buffer_time"]
        del config["max_scans"]
        cal = CalStream.from_config(Options(), "cal", config, [vis], {})
        assert cal.parameters == {}
        assert cal.buffer_time == defaults.CAL_BUFFER_TIME
        assert cal.max_scans == defaults.CAL_MAX_SCANS

    def test_too_few_antennas(self, config: Dict[str, Any]) -> None:
        vis = make_vis()
        with pytest.raises(ValueError, match="At least 4 antennas required but only 2 found"):
            CalStream.from_config(Options(), "cal", config, [vis], {})


@pytest.fixture(name="cal")
def make_cal(vis: Optional[VisStream] = None) -> CalStream:
    if vis is None:
        vis = make_vis_4ant()
    return CalStream(
        "cal",
        [vis],
        parameters={},
        buffer_time=defaults.CAL_BUFFER_TIME,
        max_scans=defaults.CAL_MAX_SCANS,
    )


class TestFlagsStream:
    """Test :class:`~.FlagsStream`."""

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sdp.flags",
            "src_streams": ["sdp_l0", "cal"],
            "rate_ratio": 10.0,
            "archive": True,
        }

    def test_from_config(self, cal: CalStream, config: Dict[str, Any]) -> None:
        vis = cal.vis
        flags = FlagsStream.from_config(Options(), "flags", config, [vis, cal], {})
        assert flags.name == "flags"
        assert flags.cal == cal
        assert flags.vis == vis
        assert flags.n_chans == vis.n_chans
        assert flags.n_baselines == vis.n_baselines
        assert flags.n_vis == vis.n_vis
        assert flags.size == vis.flag_size
        assert flags.data_rate() == vis.flag_data_rate() * 10.0
        assert flags.int_time == vis.int_time

    def test_defaults(self, cal: CalStream, config: Dict[str, Any]) -> None:
        del config["rate_ratio"]
        flags = FlagsStream.from_config(Options(), "flags", config, [cal.vis, cal], {})
        assert flags.rate_ratio == defaults.FLAGS_RATE_RATIO

    def test_incompatible_vis(self, cal: CalStream, config: Dict[str, Any]) -> None:
        vis = make_vis()
        vis.name = "bad"
        with pytest.raises(ValueError, match="src_streams bad, sdp_l0 are incompatible"):
            FlagsStream.from_config(Options(), "flags", config, [vis, cal], {})

    def test_bad_continuum_factor(self, cal: CalStream, config: Dict[str, Any]) -> None:
        vis = make_vis_4ant()
        # Need to have the same source stream to be compatible, not just a copy
        vis.src_streams = list(cal.vis.src_streams)
        vis.name = "bad"
        cal.vis.continuum_factor = 4
        with pytest.raises(
            ValueError, match="src_streams bad, sdp_l0 have incompatible continuum factors 1, 4"
        ):
            FlagsStream.from_config(Options(), "flags", config, [vis, cal], {})


@pytest.fixture(name="flags")
def make_flags(cal: CalStream) -> FlagsStream:
    cont_vis = VisStream(
        "sdp_l0_continuum",
        [cal.vis.baseline_correlation_products],
        int_time=cal.vis.int_time,
        continuum_factor=32,
        excise=cal.vis.excise,
        archive=False,
        n_servers=cal.vis.n_servers,
    )
    return FlagsStream("sdp_l0_continuum_flags", [cont_vis, cal], rate_ratio=8.0, archive=False)


class TestContinuumImageStream:
    """Test :class:`~.ContinuumImageStream`."""

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sdp.continuum_image",
            "src_streams": ["sdp_l0_continuum_flags"],
            "uvblavg_parameters": {"foo": "bar"},
            "mfimage_parameters": {"abc": 123},
            "max_realtime": 10000.0,
            "min_time": 100.0,
        }

    def test_from_config(self, flags: FlagsStream, config: Dict[str, Any]) -> None:
        cont = ContinuumImageStream.from_config(Options(), "continuum_image", config, [flags], {})
        assert cont.name == "continuum_image"
        # Make sure that the value is copied
        config["uvblavg_parameters"].clear()
        config["mfimage_parameters"].clear()
        assert cont.uvblavg_parameters == {"foo": "bar"}
        assert cont.mfimage_parameters == {"abc": 123}
        assert cont.max_realtime == 10000.0
        assert cont.min_time == 100.0
        assert cont.flags is flags
        assert cont.cal is flags.cal
        assert cont.vis is flags.vis

    def test_defaults(self, flags: FlagsStream, config: Dict[str, Any]) -> None:
        del config["uvblavg_parameters"]
        del config["mfimage_parameters"]
        del config["max_realtime"]
        del config["min_time"]
        cont = ContinuumImageStream.from_config(Options(), "continuum_image", config, [flags], {})
        assert cont.uvblavg_parameters == {}
        assert cont.mfimage_parameters == {}
        assert cont.max_realtime is None
        assert cont.min_time == defaults.CONTINUUM_MIN_TIME


@pytest.fixture(name="continuum_image")
def make_continuum_image(flags: FlagsStream) -> ContinuumImageStream:
    return ContinuumImageStream(
        "continuum_image",
        [flags],
        uvblavg_parameters={},
        mfimage_parameters={},
        max_realtime=10000.0,
        min_time=100.0,
    )


class TestSpectralImageStream:
    """Test :class:`~.SpectralImageStream`."""

    @pytest.fixture
    def config(self) -> Dict[str, Any]:
        return {
            "type": "sdp.spectral_image",
            "src_streams": ["sdp_l0_continuum_flags", "continuum_image"],
            "output_channels": [64, 100],
            "min_time": 200.0,
            "parameters": {"q_fov": 2.0},
        }

    def test_from_config(
        self, flags: FlagsStream, continuum_image: ContinuumImageStream, config: Dict[str, Any]
    ) -> None:
        spec = SpectralImageStream.from_config(
            Options(), "spectral_image", config, [flags, continuum_image], {}
        )
        # Check that it gets copied
        orig_config = copy.deepcopy(config)
        config["parameters"].clear()
        assert spec.name == "spectral_image"
        assert spec.output_channels == tuple(orig_config["output_channels"])
        assert spec.parameters == orig_config["parameters"]
        assert spec.n_chans == 36
        assert spec.flags is flags
        assert spec.vis is flags.vis
        assert spec.continuum is continuum_image

    def test_no_continuum(
        self, flags: FlagsStream, continuum_image: ContinuumImageStream, config: Dict[str, Any]
    ) -> None:
        config["src_streams"] = ["sdp_l0_continuum_flags"]
        spec = SpectralImageStream.from_config(Options(), "spectral_image", config, [flags], {})
        assert spec.continuum is None

    def test_defaults(
        self, flags: FlagsStream, continuum_image: ContinuumImageStream, config: Dict[str, Any]
    ) -> None:
        del config["parameters"]
        del config["output_channels"]
        del config["min_time"]
        spec = SpectralImageStream.from_config(
            Options(), "spectral_image", config, [flags, continuum_image], {}
        )
        assert spec.output_channels == (0, 1024)
        assert spec.min_time == defaults.SPECTRAL_MIN_TIME
        assert spec.parameters == {}


@pytest.fixture
def config() -> Dict[str, Any]:
    return {
        "version": "3.1",
        "inputs": {
            "camdata": {"type": "cam.http", "url": "http://10.8.67.235/api/client/1"},
            "i0_antenna_channelised_voltage": {
                "type": "cbf.antenna_channelised_voltage",
                "url": "spead://239.2.1.150+15:7148",
                "antennas": ["m000", "m001", "s0003", "another_antenna"],
                "instrument_dev_name": "i0",
            },
            "i0_baseline_correlation_products": {
                "type": "cbf.baseline_correlation_products",
                "url": "spead://239.9.3.1+15:7148",
                "src_streams": ["i0_antenna_channelised_voltage"],
                "instrument_dev_name": "i0",
            },
            "i0_tied_array_channelised_voltage_0x": {
                "type": "cbf.tied_array_channelised_voltage",
                "url": "spead://239.9.3.30+15:7148",
                "src_streams": ["i0_antenna_channelised_voltage"],
                "instrument_dev_name": "i0",
            },
            "i0_tied_array_channelised_voltage_0y": {
                "type": "cbf.tied_array_channelised_voltage",
                "url": "spead://239.9.3.46+7:7148",
                "src_streams": ["i0_antenna_channelised_voltage"],
                "instrument_dev_name": "i0",
            },
        },
        "outputs": {
            "l0": {
                "type": "sdp.vis",
                "src_streams": ["i0_baseline_correlation_products"],
                "output_int_time": 4.0,
                "output_channels": [0, 4096],
                "continuum_factor": 1,
                "archive": True,
            },
            "beamformer_engineering": {
                "type": "sdp.beamformer_engineering",
                "src_streams": [
                    "i0_tied_array_channelised_voltage_0x",
                    "i0_tied_array_channelised_voltage_0y",
                ],
                "output_channels": [0, 4096],
                "store": "ssd",
            },
            "cal": {"type": "sdp.cal", "src_streams": ["l0"]},
            "sdp_l1_flags": {"type": "sdp.flags", "src_streams": ["l0", "cal"], "archive": True},
            "continuum_image": {
                "type": "sdp.continuum_image",
                "src_streams": ["sdp_l1_flags"],
                "uvblavg_parameters": {},
                "mfimage_parameters": {},
                "max_realtime": 10000.0,
                "min_time": 20 * 60,
            },
            "spectral_image": {
                "type": "sdp.spectral_image",
                "src_streams": ["sdp_l1_flags", "continuum_image"],
                "output_channels": [100, 4000],
                "min_time": 3600.0,
            },
        },
        "config": {},
    }


@pytest.fixture
def config_v2() -> Dict[str, Any]:
    return {
        "version": "2.6",
        "inputs": {
            "camdata": {"type": "cam.http", "url": "http://10.8.67.235/api/client/1"},
            "i0_antenna_channelised_voltage": {
                "type": "cbf.antenna_channelised_voltage",
                "url": "spead://239.2.1.150+15:7148",
                "antennas": ["m000", "m001", "s0003", "another_antenna"],
                "n_chans": 4096,
                "n_pols": 2,
                "adc_sample_rate": 1712000000.0,
                "bandwidth": 856000000.0,
                "n_samples_between_spectra": 8192,
                "instrument_dev_name": "i0",
            },
            "i0_baseline_correlation_products": {
                "type": "cbf.baseline_correlation_products",
                "url": "spead://239.9.3.1+15:7148",
                "src_streams": ["i0_antenna_channelised_voltage"],
                "int_time": 0.499,
                "n_bls": 40,
                "xeng_out_bits_per_sample": 32,
                "n_chans_per_substream": 256,
                "instrument_dev_name": "i0",
            },
            "i0_tied_array_channelised_voltage_0x": {
                "beng_out_bits_per_sample": 8,
                "instrument_dev_name": "i0",
                "n_chans_per_substream": 256,
                "simulate": False,
                "spectra_per_heap": 256,
                "src_streams": ["i0_antenna_channelised_voltage"],
                "type": "cbf.tied_array_channelised_voltage",
                "url": "spead://239.9.3.30+15:7148",
            },
            "i0_tied_array_channelised_voltage_0y": {
                "beng_out_bits_per_sample": 8,
                "instrument_dev_name": "i0",
                "n_chans_per_substream": 256,
                "simulate": False,
                "spectra_per_heap": 256,
                "src_streams": ["i0_antenna_channelised_voltage"],
                "type": "cbf.tied_array_channelised_voltage",
                "url": "spead://239.9.3.46+7:7148",
            },
        },
        "outputs": {
            "l0": {
                "type": "sdp.vis",
                "src_streams": ["i0_baseline_correlation_products"],
                "output_int_time": 4.0,
                "output_channels": [0, 4096],
                "continuum_factor": 1,
                "archive": True,
            },
            "beamformer_engineering": {
                "type": "sdp.beamformer_engineering",
                "src_streams": [
                    "i0_tied_array_channelised_voltage_0x",
                    "i0_tied_array_channelised_voltage_0y",
                ],
                "output_channels": [0, 4096],
                "store": "ssd",
            },
            "cal": {"type": "sdp.cal", "src_streams": ["l0"]},
            "sdp_l1_flags": {
                "type": "sdp.flags",
                "src_streams": ["l0"],
                "calibration": ["cal"],
                "archive": True,
            },
            "continuum_image": {
                "type": "sdp.continuum_image",
                "src_streams": ["sdp_l1_flags"],
                "uvblavg_parameters": {},
                "mfimage_parameters": {},
                "max_realtime": 10000.0,
                "min_time": 20 * 60,
            },
            "spectral_image": {
                "type": "sdp.spectral_image",
                "src_streams": ["sdp_l1_flags", "continuum_image"],
                "output_channels": [100, 4000],
                "min_time": 3600.0,
            },
        },
        "config": {},
    }


@pytest.fixture
def config_sim() -> Dict[str, Any]:
    return {
        "version": "3.1",
        "outputs": {
            "acv": {
                "type": "sim.cbf.antenna_channelised_voltage",
                "antennas": [_M000.description, _M002.description],
                "band": "l",
                "bandwidth": 856e6,
                "centre_frequency": 1284e6,
                "adc_sample_rate": 1712e6,
                "n_chans": 4096,
            },
            "vis": {
                "type": "sdp.vis",
                "src_streams": ["bcp"],
                "output_int_time": 2.0,
                "continuum_factor": 1,
                "archive": True,
            },
            # Deliberately list the streams out of order, to check that
            # they get properly ordered
            "bcp": {
                "type": "sim.cbf.baseline_correlation_products",
                "src_streams": ["acv"],
                "n_endpoints": 16,
                "int_time": 0.5,
            },
        },
    }


class TestValidate:
    """Tests for :func:`~katsdpcontroller.product_config._validate`"""

    def test_good(self, config: Dict[str, Any]) -> None:
        product_config._validate(config)

    def test_good_v2(self, config_v2: Dict[str, Any]) -> None:
        product_config._validate(config_v2)

    def test_good_sim(self, config_v2: Dict[str, Any]) -> None:
        product_config._validate(config_v2)

    def test_bad_version(self, config: Dict[str, Any]) -> None:
        config["version"] = "1.10"
        with pytest.raises(jsonschema.ValidationError):
            product_config._validate(config)

    def test_input_bad_property(self, config: Dict[str, Any]) -> None:
        """Test that the error message on an invalid input is sensible"""
        del config["inputs"]["i0_antenna_channelised_voltage"]["instrument_dev_name"]
        with pytest.raises(
            jsonschema.ValidationError, match="'instrument_dev_name' is a required property"
        ):
            product_config._validate(config)

    def test_output_bad_property(self, config: Dict[str, Any]) -> None:
        """Test that the error message on an invalid output is sensible"""
        del config["outputs"]["l0"]["continuum_factor"]
        with pytest.raises(
            jsonschema.ValidationError, match="'continuum_factor' is a required property"
        ):
            product_config._validate(config)

    def test_input_missing_stream(self, config: Dict[str, Any]) -> None:
        """An input whose ``src_streams`` reference does not exist"""
        config["inputs"]["i0_baseline_correlation_products"]["src_streams"] = ["blah"]
        with pytest.raises(
            ValueError, match="Unknown source blah in i0_baseline_correlation_products"
        ):
            product_config._validate(config)

    def test_output_missing_stream(self, config: Dict[str, Any]) -> None:
        """An output whose ``src_streams`` reference does not exist"""
        del config["inputs"]["i0_baseline_correlation_products"]
        with pytest.raises(
            ValueError, match="Unknown source i0_baseline_correlation_products in l0"
        ):
            product_config._validate(config)

    def test_stream_wrong_type(self, config: Dict[str, Any]) -> None:
        """An entry in ``src_streams`` refers to the wrong type"""
        config["outputs"]["l0"]["src_streams"] = ["i0_antenna_channelised_voltage"]
        with pytest.raises(ValueError, match="has wrong type"):
            product_config._validate(config)

    def test_stream_name_conflict(self, config: Dict[str, Any]) -> None:
        """An input and an output have the same name"""
        config["outputs"]["i0_antenna_channelised_voltage"] = config["outputs"]["l0"]
        with pytest.raises(ValueError, match="cannot be both an input and an output"):
            product_config._validate(config)

    def test_multiple_cam_http(self, config: Dict[str, Any]) -> None:
        config["inputs"]["camdata2"] = config["inputs"]["camdata"]
        with pytest.raises(ValueError, match="have more than one cam.http"):
            product_config._validate(config)

    def test_missing_cam_http(self, config: Dict[str, Any]) -> None:
        del config["inputs"]["camdata"]
        with pytest.raises(ValueError, match="cam.http stream is required"):
            product_config._validate(config)

    def test_calibration_does_not_exist(self, config_v2: Dict[str, Any]) -> None:
        config_v2["outputs"]["sdp_l1_flags"]["calibration"] = ["bad"]
        with pytest.raises(ValueError, match="does not exist"):
            product_config._validate(config_v2)

    def test_calibration_wrong_type(self, config_v2: Dict[str, Any]) -> None:
        config_v2["outputs"]["sdp_l1_flags"]["calibration"] = ["l0"]
        with pytest.raises(ValueError, match="has wrong type"):
            product_config._validate(config_v2)

    def test_cal_models(self, config_v2: Dict[str, Any]) -> None:
        config_v2["outputs"]["cal"]["models"] = {"foo": "bar"}
        with pytest.raises(ValueError, match="no longer supports models"):
            product_config._validate(config_v2)

    def test_simulate_v2(self, config_v2: Dict[str, Any]) -> None:
        config_v2["inputs"]["i0_baseline_correlation_products"]["simulate"] = {}
        with pytest.raises(ValueError, match="simulation is not supported"):
            product_config._validate(config_v2)


class TestUpgrade:
    """Test :func:`~katsdpcontroller.product_config._upgrade`."""

    def test_simple(self, config: Dict[str, Any], config_v2: Dict[str, Any]) -> None:
        upgraded = product_config._upgrade(config_v2)
        # Uncomment to debug differences
        # import json
        # with open('actual.json', 'w') as f:
        #     json.dump(upgraded, f, indent=2, sort_keys=True)
        # with open('expected.json', 'w') as f:
        #     json.dump(config, f, indent=2, sort_keys=True)
        assert upgraded == config

    def test_upgrade_v3(self, config: Dict[str, Any]) -> None:
        upgraded = product_config._upgrade(config)
        assert upgraded == config

    def test_few_antennas(self, config: Dict[str, Any], config_v2: Dict[str, Any]) -> None:
        del config_v2["inputs"]["i0_antenna_channelised_voltage"]["antennas"][2:]
        upgraded = product_config._upgrade(config_v2)
        del config["inputs"]["i0_antenna_channelised_voltage"]["antennas"][2:]
        del config["outputs"]["cal"]
        del config["outputs"]["sdp_l1_flags"]
        del config["outputs"]["continuum_image"]
        del config["outputs"]["spectral_image"]
        assert upgraded == config

    def test_unknown_input(self, config: Dict[str, Any], config_v2: Dict[str, Any]) -> None:
        config_v2["inputs"]["xyz"] = {"type": "custom", "url": "http://test.invalid/"}
        upgraded = product_config._upgrade(config_v2)
        assert upgraded == config


class TestConfiguration:
    """Test :class:`~.Configuration`."""

    @pytest.fixture
    def config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        # Needed to make the updated config valid relative to the fake katportalclient
        del config["outputs"]["l0"]["output_channels"]
        del config["outputs"]["beamformer_engineering"]["output_channels"]
        config["outputs"]["spectral_image"]["output_channels"] = [100, 400]
        return config

    @pytest.fixture(autouse=True)
    def client(self, mocker) -> fake_katportalclient.KATPortalClient:
        # Create dummy sensors. Some deliberately have different values to
        # config_v2 to ensure that the changes are picked up.
        client = fake_katportalclient.KATPortalClient(
            components={"cbf": "cbf_1", "sub": "subarray_1"},
            sensors={
                "cbf_1_i0_antenna_channelised_voltage_n_chans": 1024,
                "cbf_1_i0_adc_sample_rate": 1088e6,
                "cbf_1_i0_antenna_channelised_voltage_n_samples_between_spectra": 2048,
                "subarray_1_streams_i0_antenna_channelised_voltage_bandwidth": 544e6,
                "subarray_1_streams_i0_antenna_channelised_voltage_centre_frequency": 816e6,
                "subarray_1_band": "u",
                "cbf_1_i0_baseline_correlation_products_int_time": 0.25,
                "cbf_1_i0_baseline_correlation_products_n_bls": 40,
                "cbf_1_i0_baseline_correlation_products_xeng_out_bits_per_sample": 32,
                "cbf_1_i0_baseline_correlation_products_n_chans_per_substream": 64,
                "cbf_1_i0_tied_array_channelised_voltage_0x_spectra_per_heap": 256,
                "cbf_1_i0_tied_array_channelised_voltage_0x_n_chans_per_substream": 64,
                "cbf_1_i0_tied_array_channelised_voltage_0x_beng_out_bits_per_sample": 8,
                "cbf_1_i0_tied_array_channelised_voltage_0y_spectra_per_heap": 256,
                "cbf_1_i0_tied_array_channelised_voltage_0y_n_chans_per_substream": 64,
                "cbf_1_i0_tied_array_channelised_voltage_0y_beng_out_bits_per_sample": 8,
            },
        )
        mocker.patch("katportalclient.KATPortalClient", return_value=client)
        return client

    async def test_sim(self, config_sim: Dict[str, Any]) -> None:
        """Test with no sensors required."""
        config = await Configuration.from_config(config_sim)
        streams = sorted(config.streams, key=lambda stream: stream.name)
        assert streams[0].name == "acv"
        assert isinstance(config.streams[0], SimAntennaChannelisedVoltageStream)
        assert streams[1].name == "bcp"
        assert isinstance(config.streams[1], SimBaselineCorrelationProductsStream)
        assert streams[2].name == "vis"
        assert isinstance(config.streams[2], VisStream)
        assert streams[2].src_streams[0] is config.streams[1]
        assert streams[1].src_streams[0] is config.streams[0]

    async def test_cyclic(self, config_sim: Dict[str, Any]) -> None:
        config_sim["outputs"]["bcp"]["src_streams"] = ["vis"]
        with pytest.raises(ValueError):
            await Configuration.from_config(config_sim)

    async def test_bad_stream(self, config: Dict[str, Any]) -> None:
        config["outputs"]["l0"]["continuum_factor"] = 3
        with pytest.raises(ValueError, match="Configuration error for stream l0: "):
            await Configuration.from_config(config)

    async def test_sensors(self, config: Dict[str, Any]) -> None:
        """Test which needs to apply sensors."""
        configuration = await Configuration.from_config(config)
        bcp = configuration.by_class(BaselineCorrelationProductsStream)[0]
        assert bcp.n_chans == 1024
        assert bcp.bandwidth == 544e6
        assert bcp.antenna_channelised_voltage.band == "u"
        assert bcp.int_time == 0.25

    async def test_connection_failed(self, client, config: Dict[str, Any]) -> None:
        with mock.patch.object(
            client, "sensor_subarray_lookup", side_effect=ConnectionRefusedError
        ):
            with pytest.raises(product_config.SensorFailure):
                await Configuration.from_config(config)

        with mock.patch.object(client, "sensor_values", side_effect=ConnectionRefusedError):
            with pytest.raises(product_config.SensorFailure):
                await Configuration.from_config(config)

    async def test_sensor_not_found(self, client, config: Dict[str, Any]) -> None:
        del client.sensors["cbf_1_i0_baseline_correlation_products_n_bls"]
        with pytest.raises(product_config.SensorFailure):
            await Configuration.from_config(config)

    async def test_sensor_bad_status(self, client, config: Dict[str, Any]) -> None:
        client.sensors[
            "cbf_1_i0_baseline_correlation_products_n_bls"
        ] = katportalclient.SensorSample(1234567890.0, 40, "unreachable")
        with pytest.raises(product_config.SensorFailure):
            await Configuration.from_config(config)

    async def test_sensor_bad_type(self, client, config: Dict[str, Any]) -> None:
        client.sensors[
            "cbf_1_i0_baseline_correlation_products_n_bls"
        ] = katportalclient.SensorSample(1234567890.0, "not a number", "nominal")
        with pytest.raises(product_config.SensorFailure):
            await Configuration.from_config(config)

    async def test_mixed_bands(self, config: Dict[str, Any]) -> None:
        config["outputs"]["l_band_sim"] = {
            "type": "sim.cbf.antenna_channelised_voltage",
            "antennas": [_M000.description, _M002.description],
            "band": "l",
            "centre_frequency": 1284e6,
            "bandwidth": 107e6,
            "adc_sample_rate": 1712e6,
            "n_chans": 32768,
        }
        with pytest.raises(ValueError, match="Only a single band is supported, found 'l', 'u'"):
            await Configuration.from_config(config)


def test_stream_classes():
    """Check that each stream in :data:~.STREAM_CLASSES` has the right ``stream_type``."""

    for name, cls in STREAM_CLASSES.items():
        assert cls.stream_type == name
