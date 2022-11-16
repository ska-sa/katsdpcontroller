"""Tests for :mod:`katsdpcontroller.product_config`."""

import copy
from unittest import mock
from typing import Dict, Optional, Any

import jsonschema
import yarl
import katpoint
import katportalclient
import pytest

from katsdpcontroller import product_config, defaults
from katsdpcontroller.product_config import (
    DigRawAntennaVoltageStream,
    SimDigRawAntennaVoltageStream,
    AntennaChannelisedVoltageStream,
    GpucbfAntennaChannelisedVoltageStream,
    SimAntennaChannelisedVoltageStream,
    BaselineCorrelationProductsStream,
    GpucbfBaselineCorrelationProductsStream,
    SimBaselineCorrelationProductsStream,
    TiedArrayChannelisedVoltageStream,
    SimTiedArrayChannelisedVoltageStream,
    CamHttpStream,
    VisStream,
    BeamformerStream,
    BeamformerEngineeringStream,
    CalStream,
    FlagsStream,
    ContinuumImageStream,
    SpectralImageStream,
    ServiceOverride,
    Options,
    Simulation,
    Configuration,
    STREAM_CLASSES
)
from . import fake_katportalclient


_M000 = katpoint.Antenna('m000, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -8.258 -207.289 1.2075 5874.184 5875.444, -0:00:39.7 0 -0:04:04.4 -0:04:53.0 0:00:57.8 -0:00:13.9 0:13:45.2 0:00:59.8, 1.14')   # noqa: E501
_M002 = katpoint.Antenna('m002, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -32.1085 -224.2365 1.248 5871.207 5872.205, 0:40:20.2 0 -0:02:41.9 -0:03:46.8 0:00:09.4 -0:00:01.1 0:03:04.7, 1.14')            # noqa: E501


class TestRecursiveDiff:
    """Test :meth:`~katsdpcontroller.product_config._recursive_diff`."""

    def test_base_add(self) -> None:
        out = product_config._recursive_diff({'a': 1}, {'a': 1, 'b': 2})
        assert out == 'b added'

    def test_base_remove(self) -> None:
        out = product_config._recursive_diff({'a': 1, 'b': 2}, {'a': 1})
        assert out == 'b removed'

    def test_base_change(self) -> None:
        out = product_config._recursive_diff({'a': 1, 'b': 2}, {'a': 1, 'b': 3})
        assert out == 'b changed from 2 to 3'

    def test_nested_add(self) -> None:
        out = product_config._recursive_diff({'x': {}}, {'x': {'a': 1}})
        assert out == 'x.a added'

    def test_nested_remove(self) -> None:
        out = product_config._recursive_diff({'x': {'a': 1}}, {'x': {}})
        assert out == 'x.a removed'

    def test_nested_change(self) -> None:
        out = product_config._recursive_diff({'x': {'a': 1, 'b': 2}}, {'x': {'a': 1, 'b': 3}})
        assert out == 'x.b changed from 2 to 3'


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
        assert product_config._url_n_endpoints('spead://239.1.2.3+7:7148') == 8
        assert product_config._url_n_endpoints('spead://239.1.2.3:7148') == 1

    def test_yarl_url(self) -> None:
        assert product_config._url_n_endpoints(yarl.URL('spead://239.1.2.3+7:7148')) == 8
        assert product_config._url_n_endpoints(yarl.URL('spead://239.1.2.3:7148')) == 1

    def test_not_spead(self) -> None:
        with pytest.raises(ValueError, match='non-spead URL http://239.1.2.3:7148'):
            product_config._url_n_endpoints('http://239.1.2.3:7148')

    def test_missing_part(self) -> None:
        with pytest.raises(ValueError, match='URL spead:/path has no host'):
            product_config._url_n_endpoints('spead:/path')
        with pytest.raises(ValueError, match='URL spead://239.1.2.3 has no port'):
            product_config._url_n_endpoints('spead://239.1.2.3')


class TestNormaliseOutputChannels:
    """Test :meth:`~katsdpcontroller.product_config.normalise_output_channels`."""

    def test_given(self) -> None:
        assert product_config._normalise_output_channels(100, (20, 80)) == (20, 80)

    def test_absent(self) -> None:
        assert product_config._normalise_output_channels(100, None) == (0, 100)

    def test_empty(self) -> None:
        with pytest.raises(ValueError, match=r'output_channels is empty \(50:50\)'):
            product_config._normalise_output_channels(100, (50, 50))
        with pytest.raises(ValueError, match=r'output_channels is empty \(60:50\)'):
            product_config._normalise_output_channels(100, (60, 50))

    def test_overflow(self) -> None:
        with pytest.raises(ValueError,
                           match=r'output_channels \(0:101\) overflows valid range 0:100'):
            product_config._normalise_output_channels(100, (0, 101))
        with pytest.raises(ValueError,
                           match=r'output_channels \(-1:1\) overflows valid range 0:100'):
            product_config._normalise_output_channels(100, (-1, 1))

    def test_align(self) -> None:
        assert product_config._normalise_output_channels(1000, (299, 301), 100) == (200, 400)
        assert product_config._normalise_output_channels(1000, (200, 400), 100) == (200, 400)

    def test_misalign(self) -> None:
        with pytest.raises(
                ValueError,
                match=r'n_chans \(789\) is not a multiple of required alignment \(100\)'):
            product_config._normalise_output_channels(789, (100, 200), 100)


class TestServiceOverride:
    """Test :class:`~.ServiceOverride`."""

    def test_from_config(self) -> None:
        config = {
            'config': {'hello': 'world'},
            'taskinfo': {'image': 'test'},
            'host': 'test.invalid'
        }
        override = ServiceOverride.from_config(config)
        assert override.config == config['config']
        assert override.taskinfo == config['taskinfo']
        assert override.host == config['host']

    def test_defaults(self) -> None:
        override = ServiceOverride.from_config({})
        assert override.config == {}
        assert override.taskinfo == {}
        assert override.host is None


class TestOptions:
    """Test :class:`~.Options`."""

    def test_from_config(self) -> None:
        config = {
            'develop': True,
            'wrapper': 'http://test.invalid/wrapper.sh',
            'service_overrides': {
                'service1': {
                    'host': 'testhost'
                }
            }
        }
        options = Options.from_config(config)
        assert options.develop_opts.any_gpu is True
        assert options.develop_opts.disable_ibv is True
        assert options.develop_opts.less_resources is True
        assert options.wrapper == config['wrapper']
        assert list(options.service_overrides.keys()) == ['service1']
        assert options.service_overrides['service1'].host == 'testhost'

    def test_defaults(self) -> None:
        options = Options.from_config({})
        assert options.develop_opts.any_gpu is False
        assert options.develop_opts.disable_ibv is False
        assert options.develop_opts.less_resources is False
        assert options.wrapper is None
        assert options.service_overrides == {}

    def test_dev_opts(self) -> None:
        config = {
            'develop': True,
            'develop_opts': {'disable_ibv': False}
        }

        options = Options.from_config(config)
        assert options.develop_opts.any_gpu is True
        assert options.develop_opts.disable_ibv is False
        assert options.develop_opts.less_resources is True


class TestSimulation:
    """Test :class:`~.Simulation`."""

    def test_from_config(self) -> None:
        config = {
            'sources': [
                'PKS 1934-63, radec, 19:39:25.03, -63:42:45.7, (200.0 12000.0 -11.11 7.777 -1.231)',
                'PKS 0408-65, radec, 4:08:20.38, -65:45:09.1, (800.0 8400.0 -3.708 3.807 -0.7202)'
            ],
            'start_time': 1234567890.0,
            'clock_ratio': 2.0
        }
        sim = Simulation.from_config(config)
        assert sim.start_time == 1234567890.0
        assert sim.clock_ratio == 2.0
        assert len(sim.sources) == 2
        assert sim.sources[0].name == 'PKS 1934-63'
        assert sim.sources[1].name == 'PKS 0408-65'

    def test_defaults(self) -> None:
        sim = Simulation.from_config({})
        assert sim.start_time is None
        assert sim.clock_ratio == 1.0
        assert sim.sources == []

    def test_invalid_source(self) -> None:
        with pytest.raises(ValueError, match='Invalid source 1: .* must have at least two fields'):
            Simulation.from_config({'sources': ['blah']})


class TestCamHttpStream:
    """Test :class:`~.CamHttpStream`."""

    def test_from_config(self) -> None:
        config = {
            'type': 'cam.http',
            'url': 'http://test.invalid'
        }
        cam_http = CamHttpStream.from_config(Options(), 'cam_data', config, [], {})
        assert cam_http.name == 'cam_data'
        assert cam_http.src_streams == []
        assert cam_http.url == yarl.URL('http://test.invalid')


class TestDigRawAntennaVoltageStream:
    """Test :class:`~.DigRawAntennaVoltageStream`."""

    def setup(self):
        self.config: Dict[str, Any] = {
            'type': 'sim.dig.raw_antenna_voltage',
            'url': 'spead://239.0.0.0+7:7148',
            'adc_sample_rate': 1712000000.0,
            'centre_frequency': 1284000000.0,
            'band': 'l',
            'antenna': 'm000'
        }

    def test_from_config(self) -> None:
        dig = DigRawAntennaVoltageStream.from_config(
            Options(), 'm000h', self.config, [], {}
        )
        assert dig.url == yarl.URL(self.config['url'])
        assert dig.adc_sample_rate == self.config['adc_sample_rate']
        assert dig.centre_frequency == self.config['centre_frequency']
        assert dig.band == self.config['band']
        assert dig.antenna_name == self.config['antenna']
        assert dig.bits_per_sample == 10
        assert dig.data_rate(1.0, 0) == 1712e7


class TestSimDigRawAntennaVoltageStream:
    """Test :class:`~.SimDigRawAntennaVoltageStream`."""

    def setup(self):
        self.config: Dict[str, Any] = {
            'type': 'sim.dig.raw_antenna_voltage',
            'adc_sample_rate': 1712000000.0,
            'centre_frequency': 1284000000.0,
            'band': 'l',
            'antenna': _M000.description
        }

    def test_from_config(self) -> None:
        dig = SimDigRawAntennaVoltageStream.from_config(
            Options(), 'm000h', self.config, [], {}
        )
        assert dig.adc_sample_rate == self.config['adc_sample_rate']
        assert dig.centre_frequency == self.config['centre_frequency']
        assert dig.band == self.config['band']
        assert dig.antenna == _M000
        assert dig.antenna_name == 'm000'
        assert dig.bits_per_sample == 10
        assert dig.data_rate(1.0, 0) == 1712e7
        assert dig.command_line_extra == []

    def test_bad_antenna_description(self) -> None:
        with pytest.raises(ValueError, match="Invalid antenna description 'bad antenna': "):
            self.config['antenna'] = 'bad antenna'
            SimDigRawAntennaVoltageStream.from_config(
                Options(), 'm000h', self.config, [], {}
            )

    def test_command_line_extra(self) -> None:
        self.config['command_line_extra'] = ['--extra-arg']
        dig = SimDigRawAntennaVoltageStream.from_config(
            Options(), 'm000h', self.config, [], {}
        )
        assert dig.command_line_extra == self.config['command_line_extra']


class TestAntennaChannelisedVoltageStream:
    """Test :class:`~.AntennaChannelisedVoltageStream`."""

    def test_from_config(self) -> None:
        config = {
            'type': 'cbf.antenna_channelised_voltage',
            'url': 'spead://239.0.0.0+7:7148',
            'antennas': ['m000', 'another_antenna'],
            'instrument_dev_name': 'narrow1'
        }
        sensors = {
            'band': 'l',
            'adc_sample_rate': 1712e6,
            'n_chans': 32768,
            'bandwidth': 107e6,
            'centre_frequency': 1284e6,
            'n_samples_between_spectra': 524288
        }
        acv = AntennaChannelisedVoltageStream.from_config(
            Options(), 'narrow1_acv', config, [], sensors
        )
        assert acv.name == 'narrow1_acv'
        assert acv.src_streams == []
        assert acv.url == yarl.URL('spead://239.0.0.0+7:7148')
        assert acv.antennas == ['m000', 'another_antenna']
        assert acv.band == 'l'
        assert acv.n_chans == 32768
        assert acv.bandwidth == 107e6
        assert acv.centre_frequency == 1284e6
        assert acv.adc_sample_rate == 1712e6
        assert acv.n_samples_between_spectra == 524288
        assert acv.instrument_dev_name == 'narrow1'


def make_dig_raw_antenna_voltage(name: str) -> DigRawAntennaVoltageStream:
    urls = {
        'm000h': 'spead2://239.1.2.0+7:7148',
        'm000v': 'spead2://239.1.2.8+7:7148',
        'm002h': 'spead2://239.1.2.16+7:7148',
        'm002v': 'spead2://239.1.2.24+7:7148'
    }
    return DigRawAntennaVoltageStream(
        name, [],
        url=yarl.URL(urls[name]),
        adc_sample_rate=1712000000.0,
        centre_frequency=1284000000.0,
        band='l',
        antenna_name=name[:-1]
    )


def make_sim_dig_raw_antenna_voltage(name: str) -> SimDigRawAntennaVoltageStream:
    return SimDigRawAntennaVoltageStream(
        name, [],
        adc_sample_rate=1712000000.0,
        centre_frequency=1284000000.0,
        band='l',
        antenna=_M000 if name.startswith('m000') else _M002
    )


class TestGpucbfAntennaChanneliseVoltageStream:
    """Test :class:`~.GpucbfAntennaChannelisedVoltageStream`."""

    def setup(self) -> None:
        self.config: Dict[str, Any] = {
            'type': 'gpucbf.antenna_channelised_voltage',
            'src_streams': ['m000h', 'm000v', 'm002h', 'm002v'],
            'n_chans': 4096
        }
        # Use a real digitiser for one of them, to test mixing
        self.src_streams = [
            (
                make_sim_dig_raw_antenna_voltage(name)
                if name != 'm002h'
                else make_dig_raw_antenna_voltage(name)
            )
            for name in self.config['src_streams']
        ]

    def test_from_config(self) -> None:
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), 'wide1_acv', self.config, self.src_streams, {}
        )
        assert acv.name == 'wide1_acv'
        assert acv.antennas == ['m000', 'm002']
        assert acv.band == self.src_streams[0].band
        assert acv.n_chans == self.config['n_chans']
        assert acv.bandwidth == self.src_streams[0].adc_sample_rate / 2
        assert acv.centre_frequency == self.src_streams[0].centre_frequency
        assert acv.adc_sample_rate == self.src_streams[0].adc_sample_rate
        assert acv.n_samples_between_spectra == 2 * self.config['n_chans']
        assert acv.sources(0) == tuple(self.src_streams[0:2])
        assert acv.sources(1) == tuple(self.src_streams[2:4])
        assert acv.data_rate(1.0, 0) == 27392e6 * 2
        assert acv.input_labels == self.config['src_streams']
        assert acv.command_line_extra == []

    def test_n_chans_not_power_of_two(self) -> None:
        for n_chans in [0, 3, 17]:
            with pytest.raises(ValueError):
                self.config['n_chans'] = n_chans
                GpucbfAntennaChannelisedVoltageStream.from_config(
                    Options(), 'wide1_acv', self.config, self.src_streams, {}
                )

    def test_too_few_channels(self) -> None:
        with pytest.raises(ValueError):
            self.config['n_chans'] = 2
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_src_streams_odd(self) -> None:
        with pytest.raises(ValueError, match='does not have an even number of elements'):
            del self.config['src_streams'][-1]
            del self.src_streams[-1]
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_band_mismatch(self) -> None:
        with pytest.raises(ValueError, match=r'Inconsistent bands \(both l and u\)'):
            self.src_streams[1].band = 'u'
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_adc_sample_rate_mismatch(self) -> None:
        with pytest.raises(
                ValueError,
                match=r'Inconsistent ADC sample rates \(both 1712000000\.0 and 1\.0\)'):
            self.src_streams[1].adc_sample_rate = 1.0
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_centre_frequency_mismatch(self) -> None:
        with pytest.raises(
                ValueError,
                match=r'Inconsistent centre frequencies \(both 1284000000\.0 and 1\.0\)'):
            self.src_streams[-1].centre_frequency = 1.0
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_input_labels(self) -> None:
        self.config['input_labels'] = ['m900h', 'm900v', 'm901h', 'm901v']
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), 'wide1_acv', self.config, self.src_streams, {}
        )
        assert acv.input_labels == self.config['input_labels']

    def test_bad_input_labels(self) -> None:
        self.config['input_labels'] = ['m900h']
        with pytest.raises(ValueError, match='input_labels has 1 elements, expected 4'):
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )
        self.config['input_labels'] = ['m900h'] * 4
        with pytest.raises(ValueError, match='are not unique'):
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_command_line_extra(self) -> None:
        self.config['command_line_extra'] = ['--extra-arg']
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), 'wide1_acv', self.config, self.src_streams, {}
        )
        assert acv.command_line_extra == self.config['command_line_extra']


class TestSimAntennaChannelisedVoltageStream:
    """Test :class:`~.SimAntennaChannelisedVoltageStream`."""

    def setup(self) -> None:
        self.config: Dict[str, Any] = {
            'type': 'sim.cbf.antenna_channelised_voltage',
            'antennas': [
                _M000.description,
                _M002.description
            ],
            'band': 'l',
            'centre_frequency': 1284e6,
            'bandwidth': 107e6,
            'adc_sample_rate': 1712e6,
            'n_chans': 32768
        }

    def test_from_config(self) -> None:
        acv = SimAntennaChannelisedVoltageStream.from_config(
            Options(), 'narrow1_acv', self.config, [], {}
        )
        assert acv.name == 'narrow1_acv'
        assert acv.src_streams == []
        assert acv.antennas == ['m000', 'm002']
        assert acv.antenna_objects == [_M000, _M002]
        assert acv.band == 'l'
        assert acv.n_chans == 32768
        assert acv.bandwidth == 107e6
        assert acv.centre_frequency == 1284e6
        assert acv.adc_sample_rate == 1712e6
        assert acv.n_samples_between_spectra == 524288

    def test_bad_bandwidth_ratio(self) -> None:
        with pytest.raises(ValueError, match='not a multiple of bandwidth'):
            self.config['bandwidth'] = 108e6
            SimAntennaChannelisedVoltageStream.from_config(
                Options(), 'narrow1_acv', self.config, [], {}
            )

    def test_bad_antenna_description(self) -> None:
        with pytest.raises(ValueError, match="Invalid antenna description 'bad antenna': "):
            self.config['antennas'][0] = 'bad antenna'
            SimAntennaChannelisedVoltageStream.from_config(
                Options(), 'narrow1_acv', self.config, [], {}
            )


def make_antenna_channelised_voltage(
        antennas=('m000', 'another_antenna')) -> AntennaChannelisedVoltageStream:
    return AntennaChannelisedVoltageStream(
        'narrow1_acv', [],
        url=yarl.URL('spead2://239.0.0.0+7:7148'),
        antennas=antennas,
        band='l',
        n_chans=32768,
        bandwidth=107e6,
        adc_sample_rate=1712e6,
        centre_frequency=1284e6,
        n_samples_between_spectra=524288,
        instrument_dev_name='narrow1'
    )


def make_sim_antenna_channelised_voltage() -> SimAntennaChannelisedVoltageStream:
    # Uses powers of two so that integration time can be computed exactly
    return SimAntennaChannelisedVoltageStream(
        'narrow1_acv', [],
        antennas=[_M000, _M002],
        band='l',
        centre_frequency=1284,
        bandwidth=256,
        adc_sample_rate=1024,
        n_chans=512
    )


def make_gpucbf_antenna_channelised_voltage() -> GpucbfAntennaChannelisedVoltageStream:
    src_streams = [
        make_sim_dig_raw_antenna_voltage(name)
        for name in ['m000h', 'm000v', 'm001h', 'm001v']
    ]
    return GpucbfAntennaChannelisedVoltageStream(
        'wide1_acv', src_streams,
        n_chans=4096
    )


class TestBaselineCorrelationProductsStream:
    """Test :class:`~.BaselineCorrelationProductsStream`."""

    def setup(self) -> None:
        self.acv = make_antenna_channelised_voltage()
        self.config = {
            'type': 'cbf.baseline_correlation_products',
            'src_streams': ['narrow1_acv'],
            'url': 'spead://239.1.0.0+7:7148',
            'instrument_dev_name': 'narrow2'
        }
        self.sensors = {
            'int_time': 0.5,
            'n_bls': 40,
            'xeng_out_bits_per_sample': 32,
            'n_chans_per_substream': 2048
        }

    def test_from_config(self) -> None:
        bcp = BaselineCorrelationProductsStream.from_config(
            Options(), 'narrow2_bcp', self.config, [self.acv], self.sensors
        )
        assert bcp.name == 'narrow2_bcp'
        assert bcp.src_streams == [self.acv]
        assert bcp.int_time == 0.5
        assert bcp.n_baselines == 40
        assert bcp.n_vis == 40 * 32768
        assert bcp.size == 40 * 32768 * 8
        assert bcp.antenna_channelised_voltage is self.acv
        assert bcp.antennas == ['m000', 'another_antenna']
        assert bcp.n_chans == 32768
        assert bcp.n_chans_per_endpoint == 4096
        assert bcp.n_substreams == 16
        assert bcp.n_antennas == 2
        assert bcp.bandwidth == 107e6
        assert bcp.centre_frequency == 1284e6
        assert bcp.adc_sample_rate == 1712e6
        assert bcp.n_samples_between_spectra == 524288
        assert bcp.data_rate(1.0, 0) == 40 * 32768 * 8 * 2 * 8

    def test_bad_endpoint_count(self) -> None:
        self.config['url'] = 'spead://239.1.0.0+8:7148'
        with pytest.raises(ValueError,
                           match=r'n_chans \(32768\) is not a multiple of endpoints \(9\)'):
            BaselineCorrelationProductsStream.from_config(
                Options(), 'narrow2_bcp', self.config, [self.acv], self.sensors
            )

    def test_bad_substream_count(self) -> None:
        self.config['url'] = 'spead://239.1.0.0+255:7148'
        with pytest.raises(ValueError,
                           match=r'channels per endpoint \(128\) is not a multiple of '
                                 r'channels per substream \(2048\)'):
            BaselineCorrelationProductsStream.from_config(
                Options(), 'narrow2_bcp', self.config, [self.acv], self.sensors
            )


class TestGpucbfBaselineCorrelationProductsStream:
    """Test :class:`~.GpucbfBaselineCorrelationProductsStream`.

    This is not as thorough as :class:`TestBaselineCorrelationProductsStream`
    because a lot of those tests are actually testing the common base class.
    """

    def setup(self) -> None:
        self.acv = make_gpucbf_antenna_channelised_voltage()
        self.config: Dict[str, Any] = {
            'type': 'gpucbf.baseline_correlation_products',
            'src_streams': 'wide1_acv',
            'int_time': 0.5
        }

    def test_from_config(self) -> None:
        bcp = GpucbfBaselineCorrelationProductsStream.from_config(
            Options(), 'wide2_bcp', self.config, [self.acv], {}
        )
        # Note: the test values will probably need to be updated as the
        # implementation evolves.
        assert bcp.n_chans_per_substream == 1024
        assert bcp.n_substreams == 4
        assert bcp.int_time == 104448 * 4096 / 856e6
        assert bcp.command_line_extra == []

    def test_command_line_extra(self) -> None:
        self.config['command_line_extra'] = ['--extra-arg']
        bcp = GpucbfBaselineCorrelationProductsStream.from_config(
            Options(), 'wide2_bcp', self.config, [self.acv], {}
        )
        assert bcp.command_line_extra == self.config['command_line_extra']


class TestSimBaselineCorrelationProductsStream:
    """Test :class:`~.SimBaselineCorrelationProductsStream`.

    This is not as thorough as :class:`TestBaselineCorrelationProductsStream`
    because a lot of those tests are actually testing the common base class.
    """

    def setup(self):
        self.acv = make_sim_antenna_channelised_voltage()
        self.config = {
            'type': 'sim.cbf.baseline_correlation_products',
            'src_streams': ['narrow1_acv'],
            'n_endpoints': 16,
            'int_time': 800,
            'n_chans_per_substream': 8
        }

    def test_from_config(self):
        bcp = SimBaselineCorrelationProductsStream.from_config(
            Options(), 'narrow2_bcp', self.config, [self.acv], {}
        )
        assert bcp.n_chans_per_substream == 8
        assert bcp.n_substreams == 64
        # Check that int_time is rounded to nearest multiple of 512
        assert bcp.int_time == 1024.0

    def test_defaults(self):
        del self.config['n_chans_per_substream']
        bcp = SimBaselineCorrelationProductsStream.from_config(
            Options(), 'narrow2_bcp', self.config, [self.acv], {}
        )
        assert bcp.n_chans_per_substream == 32
        assert bcp.n_substreams == 16


class TestTiedArrayChannelisedVoltageStream:
    """Test :class:`~.TiedArrayChannelisedVoltageStream`."""

    def setup(self) -> None:
        self.acv = make_antenna_channelised_voltage()
        self.config = {
            'type': 'cbf.tied_array_channelised_voltage',
            'src_streams': ['narrow1_acv'],
            'url': 'spead://239.2.0.0+255:7148',
            'instrument_dev_name': 'beam'
        }
        self.sensors = {
            'beng_out_bits_per_sample': 16,
            'spectra_per_heap': 256,
            'n_chans_per_substream': 64
        }

    def test_from_config(self) -> None:
        tacv = TiedArrayChannelisedVoltageStream.from_config(
            Options(), 'beam_0x', self.config, [self.acv], self.sensors
        )
        assert tacv.name == 'beam_0x'
        assert tacv.bits_per_sample == 16
        assert tacv.n_chans_per_substream == 64
        assert tacv.spectra_per_heap == 256
        assert tacv.instrument_dev_name == 'beam'
        assert tacv.size == 32768 * 256 * 2 * 2
        assert tacv.antenna_channelised_voltage is self.acv
        assert tacv.bandwidth == 107e6
        assert tacv.antennas == ['m000', 'another_antenna']
        assert round(abs(tacv.int_time * 1712e6-524288 * 256), 7) == 0


class TestSimTiedArrayChannelisedVoltageStream:
    """Test :class:`~.SimTiedArrayChannelisedVoltageStream`."""

    def setup(self) -> None:
        self.acv = make_sim_antenna_channelised_voltage()
        self.config = {
            'type': 'sim.cbf.tied_array_channelised_voltage',
            'src_streams': ['narrow1_acv'],
            'n_endpoints': 16,
            'spectra_per_heap': 256,
            'n_chans_per_substream': 4
        }

    def test_from_config(self) -> None:
        tacv = SimTiedArrayChannelisedVoltageStream.from_config(
            Options(), 'beam_0x', self.config, [self.acv], {}
        )
        assert tacv.name == 'beam_0x'
        assert tacv.bits_per_sample == 8
        assert tacv.n_chans_per_substream == 4
        assert tacv.spectra_per_heap == 256
        assert tacv.size == 512 * 256 * 2
        assert tacv.antenna_channelised_voltage is self.acv
        assert tacv.bandwidth == 256
        assert tacv.antennas == ['m000', 'm002']
        assert tacv.int_time == 512.0

    def test_defaults(self) -> None:
        del self.config['spectra_per_heap']
        del self.config['n_chans_per_substream']
        tacv = SimTiedArrayChannelisedVoltageStream.from_config(
            Options(), 'beam_0x', self.config, [self.acv], {}
        )
        assert tacv.spectra_per_heap == defaults.KATCBFSIM_SPECTRA_PER_HEAP
        assert tacv.n_chans_per_substream == 32


def make_baseline_correlation_products(
        antenna_channelised_voltage: Optional[AntennaChannelisedVoltageStream] = None
        ) -> BaselineCorrelationProductsStream:
    if antenna_channelised_voltage is None:
        antenna_channelised_voltage = make_antenna_channelised_voltage()
    return BaselineCorrelationProductsStream(
        'narrow1_bcp', [antenna_channelised_voltage],
        url=yarl.URL('spead://239.2.0.0+63:7148'),
        int_time=0.5,
        n_chans_per_substream=512,
        n_baselines=40,
        bits_per_sample=32,
        instrument_dev_name='narrow1'
    )


def make_tied_array_channelised_voltage(
        antenna_channelised_voltage: Optional[AntennaChannelisedVoltageStream],
        name: str, url: yarl.URL) -> TiedArrayChannelisedVoltageStream:
    if antenna_channelised_voltage is None:
        antenna_channelised_voltage = make_antenna_channelised_voltage()
    return TiedArrayChannelisedVoltageStream(
        name, [antenna_channelised_voltage],
        url=url,
        n_chans_per_substream=128,
        spectra_per_heap=256,
        bits_per_sample=8,
        instrument_dev_name='beam'
    )


class TestVisStream:
    """Test :class:`~.VisStream`."""

    def setup(self) -> None:
        self.bcp = make_baseline_correlation_products()
        self.config = {
            'type': 'sdp.vis',
            'src_streams': ['narrow1_bcp'],
            'output_int_time': 1.1,
            'output_channels': [128, 4096],
            'continuum_factor': 2,
            'excise': False,
            'archive': False
        }

    def test_from_config(self) -> None:
        vis = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert vis.int_time == 1.0   # Rounds to nearest multiple of CBF int_time
        assert vis.output_channels == (128, 4096)
        assert vis.continuum_factor == 2
        assert vis.excise is False
        assert vis.archive is False
        assert vis.n_servers == 4
        assert vis.baseline_correlation_products is self.bcp
        assert vis.n_chans == 1984
        assert vis.n_spectral_chans == 3968
        assert vis.n_spectral_vis == 3968 * 12
        assert vis.antennas == ['m000', 'another_antenna']
        assert vis.n_antennas == 2
        assert vis.n_pols == 2
        assert vis.n_baselines == 12
        assert vis.size == 1984 * (12 * 10 + 4)
        assert vis.flag_size == 1984 * 12
        assert vis.data_rate(1.0, 0) == vis.size / vis.int_time * 8
        assert vis.flag_data_rate(1.0, 0) == vis.flag_size / vis.int_time * 8

    def test_defaults(self) -> None:
        del self.config['output_channels']
        del self.config['excise']
        vis = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert vis.output_channels == (0, 32768)
        assert vis.excise is True

    def test_bad_continuum_factor(self) -> None:
        n_servers = 4
        continuum_factor = 3
        alignment = n_servers * continuum_factor
        self.config['continuum_factor'] = continuum_factor
        with pytest.raises(
                ValueError,
                match=fr'n_chans \({self.bcp.n_chans}\) is not a multiple of '
                      fr'required alignment \({alignment}\)'):
            VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})

    def test_misaligned_output_channels(self):
        self.config['continuum_factor'] = 2048
        vis = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert vis.output_channels == (0, 8192)

    def test_compatible(self) -> None:
        vis1 = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        self.config['continuum_factor'] = 1
        self.config['archive'] = True
        vis2 = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        del self.config['output_channels']
        vis3 = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert vis1.compatible(vis1)
        assert vis1.compatible(vis2)
        assert not vis1.compatible(vis3)

    def test_develop(self) -> None:
        options = Options(develop=True)
        vis = VisStream.from_config(options, 'sdp_l0', self.config, [self.bcp], {})
        assert vis.n_servers == 2


class TestBeamformerStream:
    """Test :class:`~.BeamformerStream`."""

    def setup(self) -> None:
        self.acv = make_antenna_channelised_voltage()
        self.tacv = [
            make_tied_array_channelised_voltage(
                self.acv, 'beam_0x', yarl.URL('spead://239.10.0.0+255:7148')),
            make_tied_array_channelised_voltage(
                self.acv, 'beam_0y', yarl.URL('spead://239.10.1.0+255:7148'))
        ]
        self.config = {
            'type': 'sdp.beamformer',
            'src_streams': ['beam_0x', 'beam_0y']
        }

    def test_from_config(self) -> None:
        bf = BeamformerStream.from_config(Options(), 'beamformer', self.config, self.tacv, {})
        assert bf.name == 'beamformer'
        assert bf.antenna_channelised_voltage.name == 'narrow1_acv'
        assert bf.tied_array_channelised_voltage == self.tacv
        assert bf.n_chans == 32768

    def test_mismatched_sources(self) -> None:
        acv = make_antenna_channelised_voltage(antennas=['m012', 's0013'])
        self.tacv[1] = make_tied_array_channelised_voltage(
            acv, 'beam_0y', yarl.URL('spead://239.10.1.0+255:7148'))
        with pytest.raises(ValueError,
                           match='Source streams do not come from the same channeliser'):
            BeamformerStream.from_config(Options(), 'beamformer', self.config, self.tacv, {})


class TestBeamformerEngineeringStream:
    def setup(self) -> None:
        self.acv = make_antenna_channelised_voltage()
        self.tacv = [
            make_tied_array_channelised_voltage(
                self.acv, 'beam_0x', yarl.URL('spead://239.10.0.0+255:7148')),
            make_tied_array_channelised_voltage(
                self.acv, 'beam_0y', yarl.URL('spead://239.10.1.0+255:7148'))
        ]
        self.config = {
            'type': 'sdp.beamformer_engineering',
            'src_streams': ['beam_0x', 'beam_0y'],
            'output_channels': [128, 1024],
            'store': 'ram'
        }

    def test_from_config(self) -> None:
        bf = BeamformerEngineeringStream.from_config(
            Options(), 'beamformer', self.config, self.tacv, {}
        )
        assert bf.name == 'beamformer'
        assert bf.antenna_channelised_voltage.name == 'narrow1_acv'
        assert bf.tied_array_channelised_voltage == self.tacv
        assert bf.store == 'ram'
        assert bf.output_channels == (128, 1024)
        assert bf.n_chans == 896

    def test_defaults(self) -> None:
        del self.config['output_channels']
        bf = BeamformerEngineeringStream.from_config(
            Options(), 'beamformer', self.config, self.tacv, {}
        )
        assert bf.output_channels == (0, 32768)
        assert bf.n_chans == 32768

    def test_misaligned_channels(self) -> None:
        self.config['output_channels'] = [1, 2]
        bf = BeamformerEngineeringStream.from_config(
            Options(), 'beamformer', self.config, self.tacv, {}
        )
        assert bf.output_channels == (0, 128)


def make_vis(
        baseline_correlation_products: Optional[BaselineCorrelationProductsStream] = None
        ) -> product_config.VisStream:
    if baseline_correlation_products is None:
        baseline_correlation_products = make_baseline_correlation_products()
    return VisStream(
        'sdp_l0', [baseline_correlation_products],
        int_time=8.0,
        continuum_factor=1,
        excise=True,
        archive=True,
        n_servers=4
    )


def make_vis_4ant() -> product_config.VisStream:
    acv = make_antenna_channelised_voltage(['m000', 'm001', 's0002', 'another_antenna'])
    bcp = make_baseline_correlation_products(acv)
    return make_vis(bcp)


class TestCalStream:
    def setup(self) -> None:
        # The default has only 2 antennas, but we need 4 to make it legal
        self.vis = make_vis_4ant()
        self.config: Dict[str, Any] = {
            'type': 'sdp.cal',
            'src_streams': ['sdp_l0'],
            'parameters': {'g_solint': 2.0},
            'buffer_time': 600.0,
            'max_scans': 100
        }

    def test_from_config(self) -> None:
        cal = CalStream.from_config(Options(), 'cal', self.config, [self.vis], {})
        assert cal.name == 'cal'
        assert cal.vis is self.vis
        # Make sure it's not referencing the original
        self.config['parameters'].clear()
        assert cal.parameters == {'g_solint': 2.0}
        assert cal.buffer_time == 600.0
        assert cal.max_scans == 100
        assert cal.n_antennas == 4
        assert cal.slots == 75

    def test_defaults(self) -> None:
        del self.config['parameters']
        del self.config['buffer_time']
        del self.config['max_scans']
        cal = CalStream.from_config(Options(), 'cal', self.config, [self.vis], {})
        assert cal.parameters == {}
        assert cal.buffer_time == defaults.CAL_BUFFER_TIME
        assert cal.max_scans == defaults.CAL_MAX_SCANS

    def test_too_few_antennas(self) -> None:
        vis = make_vis()
        with pytest.raises(ValueError, match='At least 4 antennas required but only 2 found'):
            CalStream.from_config(Options(), 'cal', self.config, [vis], {})


def make_cal(vis: Optional[VisStream] = None) -> CalStream:
    if vis is None:
        vis = make_vis_4ant()
    return CalStream(
        'cal', [vis],
        parameters={},
        buffer_time=defaults.CAL_BUFFER_TIME,
        max_scans=defaults.CAL_MAX_SCANS
    )


class TestFlagsStream:
    """Test :class:`~.FlagsStream`."""

    def setup(self) -> None:
        self.cal = make_cal()
        self.vis = self.cal.vis
        self.config = {
            'type': 'sdp.flags',
            'src_streams': ['sdp_l0', 'cal'],
            'rate_ratio': 10.0,
            'archive': True
        }

    def test_from_config(self) -> None:
        flags = FlagsStream.from_config(Options(), 'flags', self.config, [self.vis, self.cal], {})
        assert flags.name == 'flags'
        assert flags.cal == self.cal
        assert flags.vis == self.vis
        assert flags.n_chans == self.vis.n_chans
        assert flags.n_baselines == self.vis.n_baselines
        assert flags.n_vis == self.vis.n_vis
        assert flags.size == self.vis.flag_size
        assert flags.data_rate() == self.vis.flag_data_rate() * 10.0
        assert flags.int_time == self.vis.int_time

    def test_defaults(self) -> None:
        del self.config['rate_ratio']
        flags = FlagsStream.from_config(Options(), 'flags', self.config, [self.vis, self.cal], {})
        assert flags.rate_ratio == defaults.FLAGS_RATE_RATIO

    def test_incompatible_vis(self) -> None:
        vis = make_vis()
        vis.name = 'bad'
        with pytest.raises(ValueError, match='src_streams bad, sdp_l0 are incompatible'):
            FlagsStream.from_config(Options(), 'flags', self.config, [vis, self.cal], {})

    def test_bad_continuum_factor(self) -> None:
        vis = make_vis_4ant()
        # Need to have the same source stream to be compatible, not just a copy
        vis.src_streams = list(self.vis.src_streams)
        vis.name = 'bad'
        self.vis.continuum_factor = 4
        with pytest.raises(
                ValueError,
                match='src_streams bad, sdp_l0 have incompatible continuum factors 1, 4'):
            FlagsStream.from_config(Options(), 'flags', self.config, [vis, self.cal], {})


def make_flags() -> FlagsStream:
    cal = make_cal()
    cont_vis = VisStream(
        'sdp_l0_continuum', [cal.vis.baseline_correlation_products],
        int_time=cal.vis.int_time,
        continuum_factor=32,
        excise=cal.vis.excise,
        archive=False,
        n_servers=cal.vis.n_servers
    )
    return FlagsStream(
        'sdp_l0_continuum_flags', [cont_vis, cal],
        rate_ratio=8.0,
        archive=False
    )


class TestContinuumImageStream:
    """Test :class:`~.ContinuumImageStream`."""

    def setup(self) -> None:
        self.flags = make_flags()
        self.config: Dict[str, Any] = {
            'type': 'sdp.continuum_image',
            'src_streams': ['sdp_l0_continuum_flags'],
            'uvblavg_parameters': {'foo': 'bar'},
            'mfimage_parameters': {'abc': 123},
            'max_realtime': 10000.0,
            'min_time': 100.0
        }

    def test_from_config(self) -> None:
        cont = ContinuumImageStream.from_config(
            Options(), 'continuum_image', self.config, [self.flags], {}
        )
        assert cont.name == 'continuum_image'
        # Make sure that the value is copied
        self.config['uvblavg_parameters'].clear()
        self.config['mfimage_parameters'].clear()
        assert cont.uvblavg_parameters == {'foo': 'bar'}
        assert cont.mfimage_parameters == {'abc': 123}
        assert cont.max_realtime == 10000.0
        assert cont.min_time == 100.0
        assert cont.flags is self.flags
        assert cont.cal is self.flags.cal
        assert cont.vis is self.flags.vis

    def test_defaults(self) -> None:
        del self.config['uvblavg_parameters']
        del self.config['mfimage_parameters']
        del self.config['max_realtime']
        del self.config['min_time']
        cont = ContinuumImageStream.from_config(
            Options(), 'continuum_image', self.config, [self.flags], {}
        )
        assert cont.uvblavg_parameters == {}
        assert cont.mfimage_parameters == {}
        assert cont.max_realtime is None
        assert cont.min_time == defaults.CONTINUUM_MIN_TIME


def make_continuum_image(flags: Optional[FlagsStream] = None) -> ContinuumImageStream:
    if flags is None:
        flags = make_flags()
    return ContinuumImageStream(
        'continuum_image', [flags],
        uvblavg_parameters={},
        mfimage_parameters={},
        max_realtime=10000.0,
        min_time=100.0
    )


class TestSpectralImageStream:
    """Test :class:`~.SpectralImageStream`."""

    def setup(self) -> None:
        self.flags = make_flags()
        self.continuum_image = make_continuum_image()
        self.config: Dict[str, Any] = {
            'type': 'sdp.spectral_image',
            'src_streams': ['sdp_l0_continuum_flags', 'continuum_image'],
            'output_channels': [64, 100],
            'min_time': 200.0,
            'parameters': {'q_fov': 2.0}
        }

    def test_from_config(self) -> None:
        spec = SpectralImageStream.from_config(
            Options(), 'spectral_image', self.config, [self.flags, self.continuum_image], {}
        )
        # Check that it gets copied
        config = copy.deepcopy(self.config)
        self.config['parameters'].clear()
        assert spec.name == 'spectral_image'
        assert spec.output_channels == tuple(config['output_channels'])
        assert spec.parameters == config['parameters']
        assert spec.n_chans == 36
        assert spec.flags is self.flags
        assert spec.vis is self.flags.vis
        assert spec.continuum is self.continuum_image

    def test_no_continuum(self) -> None:
        self.config['src_streams'] = ['sdp_l0_continuum_flags']
        spec = SpectralImageStream.from_config(
            Options(), 'spectral_image', self.config, [self.flags], {}
        )
        assert spec.continuum is None

    def test_defaults(self) -> None:
        del self.config['parameters']
        del self.config['output_channels']
        del self.config['min_time']
        spec = SpectralImageStream.from_config(
            Options(), 'spectral_image', self.config, [self.flags, self.continuum_image], {}
        )
        assert spec.output_channels == (0, 1024)
        assert spec.min_time == defaults.SPECTRAL_MIN_TIME
        assert spec.parameters == {}


class Fixture:
    """Base class providing some sample config dicts"""

    def setup(self):
        self.config = {
            "version": "3.1",
            "inputs": {
                "camdata": {
                    "type": "cam.http",
                    "url": "http://10.8.67.235/api/client/1"
                },
                "i0_antenna_channelised_voltage": {
                    "type": "cbf.antenna_channelised_voltage",
                    "url": "spead://239.2.1.150+15:7148",
                    "antennas": ["m000", "m001", "s0003", "another_antenna"],
                    "instrument_dev_name": "i0"
                },
                "i0_baseline_correlation_products": {
                    "type": "cbf.baseline_correlation_products",
                    "url": "spead://239.9.3.1+15:7148",
                    "src_streams": ["i0_antenna_channelised_voltage"],
                    "instrument_dev_name": "i0"
                },
                "i0_tied_array_channelised_voltage_0x": {
                    "type": "cbf.tied_array_channelised_voltage",
                    "url": "spead://239.9.3.30+15:7148",
                    "src_streams": ["i0_antenna_channelised_voltage"],
                    "instrument_dev_name": "i0"
                },
                "i0_tied_array_channelised_voltage_0y": {
                    "type": "cbf.tied_array_channelised_voltage",
                    "url": "spead://239.9.3.46+7:7148",
                    "src_streams": ["i0_antenna_channelised_voltage"],
                    "instrument_dev_name": "i0"
                }
            },
            "outputs": {
                "l0": {
                    "type": "sdp.vis",
                    "src_streams": ["i0_baseline_correlation_products"],
                    "output_int_time": 4.0,
                    "output_channels": [0, 4096],
                    "continuum_factor": 1,
                    "archive": True
                },
                "beamformer_engineering": {
                    "type": "sdp.beamformer_engineering",
                    "src_streams": [
                        "i0_tied_array_channelised_voltage_0x",
                        "i0_tied_array_channelised_voltage_0y"
                    ],
                    "output_channels": [0, 4096],
                    "store": "ssd"
                },
                "cal": {
                    "type": "sdp.cal",
                    "src_streams": ["l0"]
                },
                "sdp_l1_flags": {
                    "type": "sdp.flags",
                    "src_streams": ["l0", "cal"],
                    "archive": True
                },
                "continuum_image": {
                    "type": "sdp.continuum_image",
                    "src_streams": ["sdp_l1_flags"],
                    "uvblavg_parameters": {},
                    "mfimage_parameters": {},
                    "max_realtime": 10000.0,
                    "min_time": 20 * 60
                },
                "spectral_image": {
                    "type": "sdp.spectral_image",
                    "src_streams": ["sdp_l1_flags", "continuum_image"],
                    "output_channels": [100, 4000],
                    "min_time": 3600.0
                }
            },
            "config": {}
        }
        self.config_v2 = {
            "version": "2.6",
            "inputs": {
                "camdata": {
                    "type": "cam.http",
                    "url": "http://10.8.67.235/api/client/1"
                },
                "i0_antenna_channelised_voltage": {
                    "type": "cbf.antenna_channelised_voltage",
                    "url": "spead://239.2.1.150+15:7148",
                    "antennas": ["m000", "m001", "s0003", "another_antenna"],
                    "n_chans": 4096,
                    "n_pols": 2,
                    "adc_sample_rate": 1712000000.0,
                    "bandwidth": 856000000.0,
                    "n_samples_between_spectra": 8192,
                    "instrument_dev_name": "i0"
                },
                "i0_baseline_correlation_products": {
                    "type": "cbf.baseline_correlation_products",
                    "url": "spead://239.9.3.1+15:7148",
                    "src_streams": ["i0_antenna_channelised_voltage"],
                    "int_time": 0.499,
                    "n_bls": 40,
                    "xeng_out_bits_per_sample": 32,
                    "n_chans_per_substream": 256,
                    "instrument_dev_name": "i0"
                },
                "i0_tied_array_channelised_voltage_0x": {
                    "beng_out_bits_per_sample": 8,
                    "instrument_dev_name": "i0",
                    "n_chans_per_substream": 256,
                    "simulate": False,
                    "spectra_per_heap": 256,
                    "src_streams": [
                        "i0_antenna_channelised_voltage"
                    ],
                    "type": "cbf.tied_array_channelised_voltage",
                    "url": "spead://239.9.3.30+15:7148"
                },
                "i0_tied_array_channelised_voltage_0y": {
                    "beng_out_bits_per_sample": 8,
                    "instrument_dev_name": "i0",
                    "n_chans_per_substream": 256,
                    "simulate": False,
                    "spectra_per_heap": 256,
                    "src_streams": [
                        "i0_antenna_channelised_voltage"
                    ],
                    "type": "cbf.tied_array_channelised_voltage",
                    "url": "spead://239.9.3.46+7:7148"
                }
            },
            "outputs": {
                "l0": {
                    "type": "sdp.vis",
                    "src_streams": ["i0_baseline_correlation_products"],
                    "output_int_time": 4.0,
                    "output_channels": [0, 4096],
                    "continuum_factor": 1,
                    "archive": True
                },
                "beamformer_engineering": {
                    "type": "sdp.beamformer_engineering",
                    "src_streams": [
                        "i0_tied_array_channelised_voltage_0x",
                        "i0_tied_array_channelised_voltage_0y"
                    ],
                    "output_channels": [0, 4096],
                    "store": "ssd"
                },
                "cal": {
                    "type": "sdp.cal",
                    "src_streams": ["l0"]
                },
                "sdp_l1_flags": {
                    "type": "sdp.flags",
                    "src_streams": ["l0"],
                    "calibration": ["cal"],
                    "archive": True
                },
                "continuum_image": {
                    "type": "sdp.continuum_image",
                    "src_streams": ["sdp_l1_flags"],
                    "uvblavg_parameters": {},
                    "mfimage_parameters": {},
                    "max_realtime": 10000.0,
                    "min_time": 20 * 60
                },
                "spectral_image": {
                    "type": "sdp.spectral_image",
                    "src_streams": ["sdp_l1_flags", "continuum_image"],
                    "output_channels": [100, 4000],
                    "min_time": 3600.0
                }
            },
            "config": {}
        }
        self.config_sim: Dict[str, Any] = {
            'version': '3.1',
            'outputs': {
                'acv': {
                    'type': 'sim.cbf.antenna_channelised_voltage',
                    'antennas': [
                        _M000.description,
                        _M002.description
                    ],
                    'band': 'l',
                    'bandwidth': 856e6,
                    'centre_frequency': 1284e6,
                    'adc_sample_rate': 1712e6,
                    'n_chans': 4096
                },
                'vis': {
                    'type': 'sdp.vis',
                    'src_streams': ['bcp'],
                    'output_int_time': 2.0,
                    'continuum_factor': 1,
                    'archive': True
                },
                # Deliberately list the streams out of order, to check that
                # they get properly ordered
                'bcp': {
                    'type': 'sim.cbf.baseline_correlation_products',
                    'src_streams': ['acv'],
                    'n_endpoints': 16,
                    'int_time': 0.5
                }
            }
        }


class TestValidate(Fixture):
    """Tests for :func:`~katsdpcontroller.product_config._validate`"""

    def test_good(self) -> None:
        product_config._validate(self.config)

    def test_good_v2(self) -> None:
        product_config._validate(self.config_v2)

    def test_good_sim(self) -> None:
        product_config._validate(self.config_v2)

    def test_bad_version(self) -> None:
        self.config["version"] = "1.10"
        with pytest.raises(jsonschema.ValidationError):
            product_config._validate(self.config)

    def test_input_bad_property(self) -> None:
        """Test that the error message on an invalid input is sensible"""
        del self.config["inputs"]["i0_antenna_channelised_voltage"]["instrument_dev_name"]
        with pytest.raises(jsonschema.ValidationError,
                           match="'instrument_dev_name' is a required property"):
            product_config._validate(self.config)

    def test_output_bad_property(self) -> None:
        """Test that the error message on an invalid output is sensible"""
        del self.config["outputs"]["l0"]["continuum_factor"]
        with pytest.raises(jsonschema.ValidationError,
                           match="'continuum_factor' is a required property"):
            product_config._validate(self.config)

    def test_input_missing_stream(self) -> None:
        """An input whose ``src_streams`` reference does not exist"""
        self.config["inputs"]["i0_baseline_correlation_products"]["src_streams"] = ["blah"]
        with pytest.raises(ValueError,
                           match="Unknown source blah in i0_baseline_correlation_products"):
            product_config._validate(self.config)

    def test_output_missing_stream(self) -> None:
        """An output whose ``src_streams`` reference does not exist"""
        del self.config["inputs"]["i0_baseline_correlation_products"]
        with pytest.raises(ValueError,
                           match="Unknown source i0_baseline_correlation_products in l0"):
            product_config._validate(self.config)

    def test_stream_wrong_type(self) -> None:
        """An entry in ``src_streams`` refers to the wrong type"""
        self.config["outputs"]["l0"]["src_streams"] = ["i0_antenna_channelised_voltage"]
        with pytest.raises(ValueError, match="has wrong type"):
            product_config._validate(self.config)

    def test_stream_name_conflict(self) -> None:
        """An input and an output have the same name"""
        self.config["outputs"]["i0_antenna_channelised_voltage"] = self.config["outputs"]["l0"]
        with pytest.raises(ValueError, match="cannot be both an input and an output"):
            product_config._validate(self.config)

    def test_multiple_cam_http(self) -> None:
        self.config["inputs"]["camdata2"] = self.config["inputs"]["camdata"]
        with pytest.raises(ValueError, match="have more than one cam.http"):
            product_config._validate(self.config)

    def test_missing_cam_http(self) -> None:
        del self.config["inputs"]["camdata"]
        with pytest.raises(ValueError, match="cam.http stream is required"):
            product_config._validate(self.config)

    def test_calibration_does_not_exist(self) -> None:
        self.config_v2["outputs"]["sdp_l1_flags"]["calibration"] = ["bad"]
        with pytest.raises(ValueError, match="does not exist"):
            product_config._validate(self.config_v2)

    def test_calibration_wrong_type(self) -> None:
        self.config_v2["outputs"]["sdp_l1_flags"]["calibration"] = ["l0"]
        with pytest.raises(ValueError, match="has wrong type"):
            product_config._validate(self.config_v2)

    def test_cal_models(self) -> None:
        self.config_v2["outputs"]["cal"]["models"] = {"foo": "bar"}
        with pytest.raises(ValueError, match="no longer supports models"):
            product_config._validate(self.config_v2)

    def test_simulate_v2(self) -> None:
        self.config_v2["inputs"]["i0_baseline_correlation_products"]["simulate"] = {}
        with pytest.raises(ValueError, match="simulation is not supported"):
            product_config._validate(self.config_v2)


class TestUpgrade(Fixture):
    """Test :func:`~katsdpcontroller.product_config._upgrade`."""

    def test_simple(self) -> None:
        upgraded = product_config._upgrade(self.config_v2)
        # Uncomment to debug differences
        # import json
        # with open('actual.json', 'w') as f:
        #     json.dump(upgraded, f, indent=2, sort_keys=True)
        # with open('expected.json', 'w') as f:
        #     json.dump(self.config, f, indent=2, sort_keys=True)
        assert upgraded == self.config

    def test_upgrade_v3(self) -> None:
        upgraded = product_config._upgrade(self.config)
        assert upgraded == self.config

    def test_few_antennas(self) -> None:
        del self.config_v2['inputs']['i0_antenna_channelised_voltage']['antennas'][2:]
        upgraded = product_config._upgrade(self.config_v2)
        del self.config['inputs']['i0_antenna_channelised_voltage']['antennas'][2:]
        del self.config['outputs']['cal']
        del self.config['outputs']['sdp_l1_flags']
        del self.config['outputs']['continuum_image']
        del self.config['outputs']['spectral_image']
        assert upgraded == self.config

    def test_unknown_input(self) -> None:
        self.config_v2['inputs']['xyz'] = {
            'type': 'custom',
            'url': 'http://test.invalid/'
        }
        upgraded = product_config._upgrade(self.config_v2)
        assert upgraded == self.config


class TestConfiguration(Fixture):
    """Test :class:`~.Configuration`."""

    def setup(self) -> None:
        super().setup()
        # Needed to make the updated config valid relative to the fake katportalclient
        del self.config['outputs']['l0']['output_channels']
        del self.config['outputs']['beamformer_engineering']['output_channels']
        self.config['outputs']['spectral_image']['output_channels'] = [100, 400]

    @pytest.fixture(autouse=True)
    def client(self, mocker) -> fake_katportalclient.KATPortalClient:
        # Create dummy sensors. Some deliberately have different values to
        # self.config_v2 to ensure that the changes are picked up.
        client = fake_katportalclient.KATPortalClient(
            components={'cbf': 'cbf_1', 'sub': 'subarray_1'},
            sensors={
                'cbf_1_i0_antenna_channelised_voltage_n_chans': 1024,
                'cbf_1_i0_adc_sample_rate': 1088e6,
                'cbf_1_i0_antenna_channelised_voltage_n_samples_between_spectra': 2048,
                'subarray_1_streams_i0_antenna_channelised_voltage_bandwidth': 544e6,
                'subarray_1_streams_i0_antenna_channelised_voltage_centre_frequency': 816e6,
                'subarray_1_band': 'u',
                'cbf_1_i0_baseline_correlation_products_int_time': 0.25,
                'cbf_1_i0_baseline_correlation_products_n_bls': 40,
                'cbf_1_i0_baseline_correlation_products_xeng_out_bits_per_sample': 32,
                'cbf_1_i0_baseline_correlation_products_n_chans_per_substream': 64,
                'cbf_1_i0_tied_array_channelised_voltage_0x_spectra_per_heap': 256,
                'cbf_1_i0_tied_array_channelised_voltage_0x_n_chans_per_substream': 64,
                'cbf_1_i0_tied_array_channelised_voltage_0x_beng_out_bits_per_sample': 8,
                'cbf_1_i0_tied_array_channelised_voltage_0y_spectra_per_heap': 256,
                'cbf_1_i0_tied_array_channelised_voltage_0y_n_chans_per_substream': 64,
                'cbf_1_i0_tied_array_channelised_voltage_0y_beng_out_bits_per_sample': 8
            })
        mocker.patch('katportalclient.KATPortalClient', return_value=client)
        return client

    async def test_sim(self) -> None:
        """Test with no sensors required."""
        config = await Configuration.from_config(self.config_sim)
        streams = sorted(config.streams, key=lambda stream: stream.name)
        assert streams[0].name == 'acv'
        assert isinstance(config.streams[0], SimAntennaChannelisedVoltageStream)
        assert streams[1].name == 'bcp'
        assert isinstance(config.streams[1], SimBaselineCorrelationProductsStream)
        assert streams[2].name == 'vis'
        assert isinstance(config.streams[2], VisStream)
        assert streams[2].src_streams[0] is config.streams[1]
        assert streams[1].src_streams[0] is config.streams[0]

    async def test_cyclic(self) -> None:
        self.config_sim['outputs']['bcp']['src_streams'] = ['vis']
        with pytest.raises(ValueError):
            await Configuration.from_config(self.config_sim)

    async def test_bad_stream(self) -> None:
        self.config['outputs']['l0']['continuum_factor'] = 3
        with pytest.raises(ValueError, match='Configuration error for stream l0: '):
            await Configuration.from_config(self.config)

    async def test_sensors(self) -> None:
        """Test which needs to apply sensors."""
        config = await Configuration.from_config(self.config)
        bcp = config.by_class(BaselineCorrelationProductsStream)[0]
        assert bcp.n_chans == 1024
        assert bcp.bandwidth == 544e6
        assert bcp.antenna_channelised_voltage.band == 'u'
        assert bcp.int_time == 0.25

    async def test_connection_failed(self, client) -> None:
        with mock.patch.object(client, 'sensor_subarray_lookup',
                               side_effect=ConnectionRefusedError):
            with pytest.raises(product_config.SensorFailure):
                await Configuration.from_config(self.config)

        with mock.patch.object(client, 'sensor_values',
                               side_effect=ConnectionRefusedError):
            with pytest.raises(product_config.SensorFailure):
                await Configuration.from_config(self.config)

    async def test_sensor_not_found(self, client):
        del client.sensors['cbf_1_i0_baseline_correlation_products_n_bls']
        with pytest.raises(product_config.SensorFailure):
            await Configuration.from_config(self.config)

    async def test_sensor_bad_status(self, client):
        client.sensors['cbf_1_i0_baseline_correlation_products_n_bls'] = \
            katportalclient.SensorSample(1234567890.0, 40, 'unreachable')
        with pytest.raises(product_config.SensorFailure):
            await Configuration.from_config(self.config)

    async def test_sensor_bad_type(self, client):
        client.sensors['cbf_1_i0_baseline_correlation_products_n_bls'] = \
            katportalclient.SensorSample(1234567890.0, 'not a number', 'nominal')
        with pytest.raises(product_config.SensorFailure):
            await Configuration.from_config(self.config)

    async def test_mixed_bands(self):
        self.config['outputs']['l_band_sim'] = {
            'type': 'sim.cbf.antenna_channelised_voltage',
            'antennas': [
                _M000.description,
                _M002.description
            ],
            'band': 'l',
            'centre_frequency': 1284e6,
            'bandwidth': 107e6,
            'adc_sample_rate': 1712e6,
            'n_chans': 32768
        }
        with pytest.raises(ValueError, match="Only a single band is supported, found 'l', 'u'"):
            await Configuration.from_config(self.config)


def test_stream_classes():
    """Check that each stream in :data:~.STREAM_CLASSES` has the right ``stream_type``."""

    for name, cls in STREAM_CLASSES.items():
        assert cls.stream_type == name
