"""Tests for :mod:`katsdpcontroller.product_config`."""

import copy
from unittest import mock
from typing import Dict, Optional, Any

import asynctest
import jsonschema
import yarl
import katpoint
import katportalclient
from nose.tools import (
    assert_equal, assert_almost_equal, assert_is, assert_is_none, assert_is_instance,
    assert_true, assert_false, assert_raises, assert_raises_regex
)

from .. import product_config, defaults
from ..product_config import (
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
        assert_equal(out, 'b added')

    def test_base_remove(self) -> None:
        out = product_config._recursive_diff({'a': 1, 'b': 2}, {'a': 1})
        assert_equal(out, 'b removed')

    def test_base_change(self) -> None:
        out = product_config._recursive_diff({'a': 1, 'b': 2}, {'a': 1, 'b': 3})
        assert_equal(out, 'b changed from 2 to 3')

    def test_nested_add(self) -> None:
        out = product_config._recursive_diff({'x': {}}, {'x': {'a': 1}})
        assert_equal(out, 'x.a added')

    def test_nested_remove(self) -> None:
        out = product_config._recursive_diff({'x': {'a': 1}}, {'x': {}})
        assert_equal(out, 'x.a removed')

    def test_nested_change(self) -> None:
        out = product_config._recursive_diff({'x': {'a': 1, 'b': 2}}, {'x': {'a': 1, 'b': 3}})
        assert_equal(out, 'x.b changed from 2 to 3')


class TestOverride:
    """Test :meth:`~katsdpcontroller.product_config.override`."""

    def test_add(self) -> None:
        out = product_config.override({"a": 1}, {"b": 2})
        assert_equal({"a": 1, "b": 2}, out)

    def test_remove(self) -> None:
        out = product_config.override({"a": 1, "b": 2}, {"b": None})
        assert_equal({"a": 1}, out)
        out = product_config.override(out, {"b": None})  # Already absent
        assert_equal({"a": 1}, out)

    def test_replace(self) -> None:
        out = product_config.override({"a": 1, "b": 2}, {"b": 3})
        assert_equal({"a": 1, "b": 3}, out)

    def test_recurse(self) -> None:
        orig = {"a": {"aa": 1, "ab": 2}, "b": {"ba": {"c": 10}, "bb": [5]}}
        override = {"a": {"aa": [], "ab": None, "ac": 3}, "b": {"bb": [1, 2]}}
        out = product_config.override(orig, override)
        assert_equal({"a": {"aa": [], "ac": 3}, "b": {"ba": {"c": 10}, "bb": [1, 2]}}, out)


class TestUrlNEndpoints:
    """Test :meth:`~katsdpcontroller.product_config._url_n_endpoints`."""

    def test_simple(self) -> None:
        assert_equal(product_config._url_n_endpoints('spead://239.1.2.3+7:7148'), 8)
        assert_equal(product_config._url_n_endpoints('spead://239.1.2.3:7148'), 1)

    def test_yarl_url(self) -> None:
        assert_equal(product_config._url_n_endpoints(yarl.URL('spead://239.1.2.3+7:7148')), 8)
        assert_equal(product_config._url_n_endpoints(yarl.URL('spead://239.1.2.3:7148')), 1)

    def test_not_spead(self) -> None:
        with assert_raises_regex(ValueError, 'non-spead URL http://239.1.2.3:7148'):
            product_config._url_n_endpoints('http://239.1.2.3:7148')

    def test_missing_part(self) -> None:
        with assert_raises_regex(ValueError, 'URL spead:/path has no host'):
            product_config._url_n_endpoints('spead:/path')
        with assert_raises_regex(ValueError, 'URL spead://239.1.2.3 has no port'):
            product_config._url_n_endpoints('spead://239.1.2.3')


class TestNormaliseOutputChannels:
    """Test :meth:`~katsdpcontroller.product_config.normalise_output_channels`."""

    def test_given(self) -> None:
        assert_equal(product_config._normalise_output_channels(100, (20, 80)), (20, 80))

    def test_absent(self) -> None:
        assert_equal(product_config._normalise_output_channels(100, None), (0, 100))

    def test_empty(self) -> None:
        with assert_raises_regex(ValueError, r'output_channels is empty \(50:50\)'):
            product_config._normalise_output_channels(100, (50, 50))
        with assert_raises_regex(ValueError, r'output_channels is empty \(60:50\)'):
            product_config._normalise_output_channels(100, (60, 50))

    def test_overflow(self) -> None:
        with assert_raises_regex(ValueError,
                                 r'output_channels \(0:101\) overflows valid range 0:100'):
            product_config._normalise_output_channels(100, (0, 101))
        with assert_raises_regex(ValueError,
                                 r'output_channels \(-1:1\) overflows valid range 0:100'):
            product_config._normalise_output_channels(100, (-1, 1))

    def test_align(self) -> None:
        assert_equal(product_config._normalise_output_channels(1000, (299, 301), 100), (200, 400))
        assert_equal(product_config._normalise_output_channels(1000, (200, 400), 100), (200, 400))

    def test_misalign(self) -> None:
        with assert_raises_regex(
                ValueError,
                r'n_chans \(789\) is not a multiple of required alignment \(100\)'):
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
        assert_equal(override.config, config['config'])
        assert_equal(override.taskinfo, config['taskinfo'])
        assert_equal(override.host, config['host'])

    def test_defaults(self) -> None:
        override = ServiceOverride.from_config({})
        assert_equal(override.config, {})
        assert_equal(override.taskinfo, {})
        assert_equal(override.host, None)


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
        assert_equal(options.develop, config['develop'])
        assert_equal(options.wrapper, config['wrapper'])
        assert_equal(list(options.service_overrides.keys()), ['service1'])
        assert_equal(options.service_overrides['service1'].host, 'testhost')

    def test_defaults(self) -> None:
        options = Options.from_config({})
        assert_equal(options.develop, False)
        assert_equal(options.wrapper, None)
        assert_equal(options.service_overrides, {})


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
        assert_equal(sim.start_time, 1234567890.0)
        assert_equal(sim.clock_ratio, 2.0)
        assert_equal(len(sim.sources), 2)
        assert_equal(sim.sources[0].name, 'PKS 1934-63')
        assert_equal(sim.sources[1].name, 'PKS 0408-65')

    def test_defaults(self) -> None:
        sim = Simulation.from_config({})
        assert_is_none(sim.start_time)
        assert_equal(sim.clock_ratio, 1.0)
        assert_equal(sim.sources, [])

    def test_invalid_source(self) -> None:
        with assert_raises_regex(ValueError, 'Invalid source 1: .* must have at least two fields'):
            Simulation.from_config({'sources': ['blah']})


class TestCamHttpStream:
    """Test :class:`~.CamHttpStream`."""

    def test_from_config(self) -> None:
        config = {
            'type': 'cam.http',
            'url': 'http://test.invalid'
        }
        cam_http = CamHttpStream.from_config(Options(), 'cam_data', config, [], {})
        assert_equal(cam_http.name, 'cam_data')
        assert_equal(cam_http.src_streams, [])
        assert_equal(cam_http.url, yarl.URL('http://test.invalid'))


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
        assert_equal(dig.url, yarl.URL(self.config['url']))
        assert_equal(dig.adc_sample_rate, self.config['adc_sample_rate'])
        assert_equal(dig.centre_frequency, self.config['centre_frequency'])
        assert_equal(dig.band, self.config['band'])
        assert_equal(dig.antenna_name, self.config['antenna'])
        assert_equal(dig.bits_per_sample, 10)
        assert_equal(dig.data_rate(1.0, 0), 1712e7)


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
        assert_equal(dig.adc_sample_rate, self.config['adc_sample_rate'])
        assert_equal(dig.centre_frequency, self.config['centre_frequency'])
        assert_equal(dig.band, self.config['band'])
        assert_equal(dig.antenna, _M000)
        assert_equal(dig.antenna_name, 'm000')
        assert_equal(dig.bits_per_sample, 10)
        assert_equal(dig.data_rate(1.0, 0), 1712e7)
        assert_equal(dig.command_line_extra, [])

    def test_bad_antenna_description(self) -> None:
        with assert_raises_regex(ValueError, "Invalid antenna description 'bad antenna': "):
            self.config['antenna'] = 'bad antenna'
            SimDigRawAntennaVoltageStream.from_config(
                Options(), 'm000h', self.config, [], {}
            )

    def test_command_line_extra(self) -> None:
        self.config['command_line_extra'] = ['--extra-arg']
        dig = SimDigRawAntennaVoltageStream.from_config(
            Options(), 'm000h', self.config, [], {}
        )
        assert_equal(dig.command_line_extra, self.config['command_line_extra'])


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
        assert_equal(acv.name, 'narrow1_acv')
        assert_equal(acv.src_streams, [])
        assert_equal(acv.url, yarl.URL('spead://239.0.0.0+7:7148'))
        assert_equal(acv.antennas, ['m000', 'another_antenna'])
        assert_equal(acv.band, 'l')
        assert_equal(acv.n_chans, 32768)
        assert_equal(acv.bandwidth, 107e6)
        assert_equal(acv.centre_frequency, 1284e6)
        assert_equal(acv.adc_sample_rate, 1712e6)
        assert_equal(acv.n_samples_between_spectra, 524288)
        assert_equal(acv.instrument_dev_name, 'narrow1')


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
        assert_equal(acv.name, 'wide1_acv')
        assert_equal(acv.antennas, ['m000', 'm002'])
        assert_equal(acv.band, self.src_streams[0].band)
        assert_equal(acv.n_chans, self.config['n_chans'])
        assert_equal(acv.bandwidth, self.src_streams[0].adc_sample_rate / 2)
        assert_equal(acv.centre_frequency, self.src_streams[0].centre_frequency)
        assert_equal(acv.adc_sample_rate, self.src_streams[0].adc_sample_rate)
        assert_equal(acv.n_samples_between_spectra, 2 * self.config['n_chans'])
        assert_equal(acv.sources(0), tuple(self.src_streams[0:2]))
        assert_equal(acv.sources(1), tuple(self.src_streams[2:4]))
        assert_equal(acv.data_rate(1.0, 0), 27392e6)
        assert_equal(acv.input_labels, self.config['src_streams'])
        assert_equal(acv.command_line_extra, [])

    def test_n_chans_not_power_of_two(self) -> None:
        for n_chans in [0, 3, 17]:
            with assert_raises(ValueError):
                self.config['n_chans'] = n_chans
                GpucbfAntennaChannelisedVoltageStream.from_config(
                    Options(), 'wide1_acv', self.config, self.src_streams, {}
                )

    def test_too_few_channels(self) -> None:
        with assert_raises(ValueError):
            self.config['n_chans'] = 2
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_src_streams_odd(self) -> None:
        with assert_raises_regex(ValueError, 'does not have an even number of elements'):
            del self.config['src_streams'][-1]
            del self.src_streams[-1]
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_band_mismatch(self) -> None:
        with assert_raises_regex(ValueError, r'Inconsistent bands \(both l and u\)'):
            self.src_streams[1].band = 'u'
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_adc_sample_rate_mismatch(self) -> None:
        with assert_raises_regex(
                ValueError,
                r'Inconsistent ADC sample rates \(both 1712000000\.0 and 1\.0\)'):
            self.src_streams[1].adc_sample_rate = 1.0
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_centre_frequency_mismatch(self) -> None:
        with assert_raises_regex(
                ValueError,
                r'Inconsistent centre frequencies \(both 1284000000\.0 and 1\.0\)'):
            self.src_streams[-1].centre_frequency = 1.0
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_input_labels(self) -> None:
        self.config['input_labels'] = ['m900h', 'm900v', 'm901h', 'm901v']
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), 'wide1_acv', self.config, self.src_streams, {}
        )
        assert_equal(acv.input_labels, self.config['input_labels'])

    def test_bad_input_labels(self) -> None:
        self.config['input_labels'] = ['m900h']
        with assert_raises_regex(ValueError, 'input_labels has 1 elements, expected 4'):
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )
        self.config['input_labels'] = ['m900h'] * 4
        with assert_raises_regex(ValueError, 'are not unique'):
            GpucbfAntennaChannelisedVoltageStream.from_config(
                Options(), 'wide1_acv', self.config, self.src_streams, {}
            )

    def test_command_line_extra(self) -> None:
        self.config['command_line_extra'] = ['--extra-arg']
        acv = GpucbfAntennaChannelisedVoltageStream.from_config(
            Options(), 'wide1_acv', self.config, self.src_streams, {}
        )
        assert_equal(acv.command_line_extra, self.config['command_line_extra'])


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
        assert_equal(acv.name, 'narrow1_acv')
        assert_equal(acv.src_streams, [])
        assert_equal(acv.antennas, ['m000', 'm002'])
        assert_equal(acv.antenna_objects, [_M000, _M002])
        assert_equal(acv.band, 'l')
        assert_equal(acv.n_chans, 32768)
        assert_equal(acv.bandwidth, 107e6)
        assert_equal(acv.centre_frequency, 1284e6)
        assert_equal(acv.adc_sample_rate, 1712e6)
        assert_equal(acv.n_samples_between_spectra, 524288)

    def test_bad_bandwidth_ratio(self) -> None:
        with assert_raises_regex(ValueError, 'not a multiple of bandwidth'):
            self.config['bandwidth'] = 108e6
            SimAntennaChannelisedVoltageStream.from_config(
                Options(), 'narrow1_acv', self.config, [], {}
            )

    def test_bad_antenna_description(self) -> None:
        with assert_raises_regex(ValueError, "Invalid antenna description 'bad antenna': "):
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
        assert_equal(bcp.name, 'narrow2_bcp')
        assert_equal(bcp.src_streams, [self.acv])
        assert_equal(bcp.int_time, 0.5)
        assert_equal(bcp.n_baselines, 40)
        assert_equal(bcp.n_vis, 40 * 32768)
        assert_equal(bcp.size, 40 * 32768 * 8)
        assert_is(bcp.antenna_channelised_voltage, self.acv)
        assert_equal(bcp.antennas, ['m000', 'another_antenna'])
        assert_equal(bcp.n_chans, 32768)
        assert_equal(bcp.n_chans_per_endpoint, 4096)
        assert_equal(bcp.n_substreams, 16)
        assert_equal(bcp.n_antennas, 2)
        assert_equal(bcp.bandwidth, 107e6)
        assert_equal(bcp.centre_frequency, 1284e6)
        assert_equal(bcp.adc_sample_rate, 1712e6)
        assert_equal(bcp.n_samples_between_spectra, 524288)
        assert_equal(bcp.data_rate(1.0, 0), 40 * 32768 * 8 * 2 * 8)

    def test_bad_endpoint_count(self) -> None:
        self.config['url'] = 'spead://239.1.0.0+8:7148'
        with assert_raises_regex(ValueError,
                                 r'n_chans \(32768\) is not a multiple of endpoints \(9\)'):
            BaselineCorrelationProductsStream.from_config(
                Options(), 'narrow2_bcp', self.config, [self.acv], self.sensors
            )

    def test_bad_substream_count(self) -> None:
        self.config['url'] = 'spead://239.1.0.0+255:7148'
        with assert_raises_regex(ValueError,
                                 r'channels per endpoint \(128\) is not a multiple of '
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
        assert_equal(bcp.n_chans_per_substream, 512)
        assert_equal(bcp.n_substreams, 8)
        assert_equal(bcp.int_time, 104448 * 4096 / 856e6)
        assert_equal(bcp.command_line_extra, [])

    def test_command_line_extra(self) -> None:
        self.config['command_line_extra'] = ['--extra-arg']
        bcp = GpucbfBaselineCorrelationProductsStream.from_config(
            Options(), 'wide2_bcp', self.config, [self.acv], {}
        )
        assert_equal(bcp.command_line_extra, self.config['command_line_extra'])


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
        assert_equal(bcp.n_chans_per_substream, 8)
        assert_equal(bcp.n_substreams, 64)
        # Check that int_time is rounded to nearest multiple of 512
        assert_equal(bcp.int_time, 1024.0)

    def test_defaults(self):
        del self.config['n_chans_per_substream']
        bcp = SimBaselineCorrelationProductsStream.from_config(
            Options(), 'narrow2_bcp', self.config, [self.acv], {}
        )
        assert_equal(bcp.n_chans_per_substream, 32)
        assert_equal(bcp.n_substreams, 16)


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
        assert_equal(tacv.name, 'beam_0x')
        assert_equal(tacv.bits_per_sample, 16)
        assert_equal(tacv.n_chans_per_substream, 64)
        assert_equal(tacv.spectra_per_heap, 256)
        assert_equal(tacv.instrument_dev_name, 'beam')
        assert_equal(tacv.size, 32768 * 256 * 2 * 2)
        assert_is(tacv.antenna_channelised_voltage, self.acv)
        assert_equal(tacv.bandwidth, 107e6)
        assert_equal(tacv.antennas, ['m000', 'another_antenna'])
        assert_almost_equal(tacv.int_time * 1712e6, 524288 * 256)


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
        assert_equal(tacv.name, 'beam_0x')
        assert_equal(tacv.bits_per_sample, 8)
        assert_equal(tacv.n_chans_per_substream, 4)
        assert_equal(tacv.spectra_per_heap, 256)
        assert_equal(tacv.size, 512 * 256 * 2)
        assert_is(tacv.antenna_channelised_voltage, self.acv)
        assert_equal(tacv.bandwidth, 256)
        assert_equal(tacv.antennas, ['m000', 'm002'])
        assert_equal(tacv.int_time, 512.0)

    def test_defaults(self) -> None:
        del self.config['spectra_per_heap']
        del self.config['n_chans_per_substream']
        tacv = SimTiedArrayChannelisedVoltageStream.from_config(
            Options(), 'beam_0x', self.config, [self.acv], {}
        )
        assert_equal(tacv.spectra_per_heap, defaults.KATCBFSIM_SPECTRA_PER_HEAP)
        assert_equal(tacv.n_chans_per_substream, 32)


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
        assert_equal(vis.int_time, 1.0)   # Rounds to nearest multiple of CBF int_time
        assert_equal(vis.output_channels, (128, 4096))
        assert_equal(vis.continuum_factor, 2)
        assert_equal(vis.excise, False)
        assert_equal(vis.archive, False)
        assert_equal(vis.n_servers, 4)
        assert_is(vis.baseline_correlation_products, self.bcp)
        assert_equal(vis.n_chans, 1984)
        assert_equal(vis.n_spectral_chans, 3968)
        assert_equal(vis.n_spectral_vis, 3968 * 12)
        assert_equal(vis.antennas, ['m000', 'another_antenna'])
        assert_equal(vis.n_antennas, 2)
        assert_equal(vis.n_pols, 2)
        assert_equal(vis.n_baselines, 12)
        assert_equal(vis.size, 1984 * (12 * 10 + 4))
        assert_equal(vis.flag_size, 1984 * 12)
        assert_equal(vis.data_rate(1.0, 0), vis.size / vis.int_time * 8)
        assert_equal(vis.flag_data_rate(1.0, 0), vis.flag_size / vis.int_time * 8)

    def test_defaults(self) -> None:
        del self.config['output_channels']
        del self.config['excise']
        vis = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert_equal(vis.output_channels, (0, 32768))
        assert_equal(vis.excise, True)

    def test_bad_continuum_factor(self) -> None:
        n_servers = 4
        continuum_factor = 3
        alignment = n_servers * continuum_factor
        self.config['continuum_factor'] = continuum_factor
        with assert_raises_regex(
                ValueError,
                fr'n_chans \({self.bcp.n_chans}\) is not a multiple of '
                fr'required alignment \({alignment}\)'):
            VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})

    def test_misaligned_output_channels(self):
        self.config['continuum_factor'] = 2048
        vis = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert_equal(vis.output_channels, (0, 8192))

    def test_compatible(self) -> None:
        vis1 = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        self.config['continuum_factor'] = 1
        self.config['archive'] = True
        vis2 = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        del self.config['output_channels']
        vis3 = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert_true(vis1.compatible(vis1))
        assert_true(vis1.compatible(vis2))
        assert_false(vis1.compatible(vis3))

    def test_develop(self) -> None:
        options = Options(develop=True)
        vis = VisStream.from_config(options, 'sdp_l0', self.config, [self.bcp], {})
        assert_equal(vis.n_servers, 2)


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
        assert_equal(bf.name, 'beamformer')
        assert_equal(bf.antenna_channelised_voltage.name, 'narrow1_acv')
        assert_equal(bf.tied_array_channelised_voltage, self.tacv)
        assert_equal(bf.n_chans, 32768)

    def test_mismatched_sources(self) -> None:
        acv = make_antenna_channelised_voltage(antennas=['m012', 's0013'])
        self.tacv[1] = make_tied_array_channelised_voltage(
            acv, 'beam_0y', yarl.URL('spead://239.10.1.0+255:7148'))
        with assert_raises_regex(ValueError,
                                 'Source streams do not come from the same channeliser'):
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
        assert_equal(bf.name, 'beamformer')
        assert_equal(bf.antenna_channelised_voltage.name, 'narrow1_acv')
        assert_equal(bf.tied_array_channelised_voltage, self.tacv)
        assert_equal(bf.store, 'ram')
        assert_equal(bf.output_channels, (128, 1024))
        assert_equal(bf.n_chans, 896)

    def test_defaults(self) -> None:
        del self.config['output_channels']
        bf = BeamformerEngineeringStream.from_config(
            Options(), 'beamformer', self.config, self.tacv, {}
        )
        assert_equal(bf.output_channels, (0, 32768))
        assert_equal(bf.n_chans, 32768)

    def test_misaligned_channels(self) -> None:
        self.config['output_channels'] = [1, 2]
        bf = BeamformerEngineeringStream.from_config(
            Options(), 'beamformer', self.config, self.tacv, {}
        )
        assert_equal(bf.output_channels, (0, 128))


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
        assert_equal(cal.name, 'cal')
        assert_is(cal.vis, self.vis)
        # Make sure it's not referencing the original
        self.config['parameters'].clear()
        assert_equal(cal.parameters, {'g_solint': 2.0})
        assert_equal(cal.buffer_time, 600.0)
        assert_equal(cal.max_scans, 100)
        assert_equal(cal.n_antennas, 4)
        assert_equal(cal.slots, 75)

    def test_defaults(self) -> None:
        del self.config['parameters']
        del self.config['buffer_time']
        del self.config['max_scans']
        cal = CalStream.from_config(Options(), 'cal', self.config, [self.vis], {})
        assert_equal(cal.parameters, {})
        assert_equal(cal.buffer_time, defaults.CAL_BUFFER_TIME)
        assert_equal(cal.max_scans, defaults.CAL_MAX_SCANS)

    def test_too_few_antennas(self) -> None:
        vis = make_vis()
        with assert_raises_regex(ValueError, 'At least 4 antennas required but only 2 found'):
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
        assert_equal(flags.name, 'flags')
        assert_equal(flags.cal, self.cal)
        assert_equal(flags.vis, self.vis)
        assert_equal(flags.n_chans, self.vis.n_chans)
        assert_equal(flags.n_baselines, self.vis.n_baselines)
        assert_equal(flags.n_vis, self.vis.n_vis)
        assert_equal(flags.size, self.vis.flag_size)
        assert_equal(flags.data_rate(), self.vis.flag_data_rate() * 10.0)
        assert_equal(flags.int_time, self.vis.int_time)

    def test_defaults(self) -> None:
        del self.config['rate_ratio']
        flags = FlagsStream.from_config(Options(), 'flags', self.config, [self.vis, self.cal], {})
        assert_equal(flags.rate_ratio, defaults.FLAGS_RATE_RATIO)

    def test_incompatible_vis(self) -> None:
        vis = make_vis()
        vis.name = 'bad'
        with assert_raises_regex(ValueError, 'src_streams bad, sdp_l0 are incompatible'):
            FlagsStream.from_config(Options(), 'flags', self.config, [vis, self.cal], {})

    def test_bad_continuum_factor(self) -> None:
        vis = make_vis_4ant()
        # Need to have the same source stream to be compatible, not just a copy
        vis.src_streams = list(self.vis.src_streams)
        vis.name = 'bad'
        self.vis.continuum_factor = 4
        with assert_raises_regex(
                ValueError,
                'src_streams bad, sdp_l0 have incompatible continuum factors 1, 4'):
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
        assert_equal(cont.name, 'continuum_image')
        # Make sure that the value is copied
        self.config['uvblavg_parameters'].clear()
        self.config['mfimage_parameters'].clear()
        assert_equal(cont.uvblavg_parameters, {'foo': 'bar'})
        assert_equal(cont.mfimage_parameters, {'abc': 123})
        assert_equal(cont.max_realtime, 10000.0)
        assert_equal(cont.min_time, 100.0)
        assert_is(cont.flags, self.flags)
        assert_is(cont.cal, self.flags.cal)
        assert_is(cont.vis, self.flags.vis)

    def test_defaults(self) -> None:
        del self.config['uvblavg_parameters']
        del self.config['mfimage_parameters']
        del self.config['max_realtime']
        del self.config['min_time']
        cont = ContinuumImageStream.from_config(
            Options(), 'continuum_image', self.config, [self.flags], {}
        )
        assert_equal(cont.uvblavg_parameters, {})
        assert_equal(cont.mfimage_parameters, {})
        assert_equal(cont.max_realtime, None)
        assert_equal(cont.min_time, defaults.CONTINUUM_MIN_TIME)


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
        assert_equal(spec.name, 'spectral_image')
        assert_equal(spec.output_channels, tuple(config['output_channels']))
        assert_equal(spec.parameters, config['parameters'])
        assert_equal(spec.n_chans, 36)
        assert_is(spec.flags, self.flags)
        assert_is(spec.vis, self.flags.vis)
        assert_is(spec.continuum, self.continuum_image)

    def test_no_continuum(self) -> None:
        self.config['src_streams'] = ['sdp_l0_continuum_flags']
        spec = SpectralImageStream.from_config(
            Options(), 'spectral_image', self.config, [self.flags], {}
        )
        assert_is_none(spec.continuum)

    def test_defaults(self) -> None:
        del self.config['parameters']
        del self.config['output_channels']
        del self.config['min_time']
        spec = SpectralImageStream.from_config(
            Options(), 'spectral_image', self.config, [self.flags, self.continuum_image], {}
        )
        assert_equal(spec.output_channels, (0, 1024))
        assert_equal(spec.min_time, defaults.SPECTRAL_MIN_TIME)
        assert_equal(spec.parameters, {})


class Fixture(asynctest.TestCase):
    """Base class providing some sample config dicts"""

    def setUp(self):
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
        with assert_raises(jsonschema.ValidationError):
            product_config._validate(self.config)

    def test_input_bad_property(self) -> None:
        """Test that the error message on an invalid input is sensible"""
        del self.config["inputs"]["i0_antenna_channelised_voltage"]["instrument_dev_name"]
        with assert_raises_regex(jsonschema.ValidationError,
                                 "'instrument_dev_name' is a required property"):
            product_config._validate(self.config)

    def test_output_bad_property(self) -> None:
        """Test that the error message on an invalid output is sensible"""
        del self.config["outputs"]["l0"]["continuum_factor"]
        with assert_raises_regex(jsonschema.ValidationError,
                                 "'continuum_factor' is a required property"):
            product_config._validate(self.config)

    def test_input_missing_stream(self) -> None:
        """An input whose ``src_streams`` reference does not exist"""
        self.config["inputs"]["i0_baseline_correlation_products"]["src_streams"] = ["blah"]
        with assert_raises_regex(ValueError,
                                 "Unknown source blah in i0_baseline_correlation_products"):
            product_config._validate(self.config)

    def test_output_missing_stream(self) -> None:
        """An output whose ``src_streams`` reference does not exist"""
        del self.config["inputs"]["i0_baseline_correlation_products"]
        with assert_raises_regex(ValueError,
                                 "Unknown source i0_baseline_correlation_products in l0"):
            product_config._validate(self.config)

    def test_stream_wrong_type(self) -> None:
        """An entry in ``src_streams`` refers to the wrong type"""
        self.config["outputs"]["l0"]["src_streams"] = ["i0_antenna_channelised_voltage"]
        with assert_raises_regex(ValueError, "has wrong type"):
            product_config._validate(self.config)

    def test_stream_name_conflict(self) -> None:
        """An input and an output have the same name"""
        self.config["outputs"]["i0_antenna_channelised_voltage"] = self.config["outputs"]["l0"]
        with assert_raises_regex(ValueError,
                                 "cannot be both an input and an output"):
            product_config._validate(self.config)

    def test_multiple_cam_http(self) -> None:
        self.config["inputs"]["camdata2"] = self.config["inputs"]["camdata"]
        with assert_raises_regex(ValueError, "have more than one cam.http"):
            product_config._validate(self.config)

    def test_missing_cam_http(self) -> None:
        del self.config["inputs"]["camdata"]
        with assert_raises_regex(ValueError, "cam.http stream is required"):
            product_config._validate(self.config)

    def test_calibration_does_not_exist(self) -> None:
        self.config_v2["outputs"]["sdp_l1_flags"]["calibration"] = ["bad"]
        with assert_raises_regex(ValueError, "does not exist"):
            product_config._validate(self.config_v2)

    def test_calibration_wrong_type(self) -> None:
        self.config_v2["outputs"]["sdp_l1_flags"]["calibration"] = ["l0"]
        with assert_raises_regex(ValueError, "has wrong type"):
            product_config._validate(self.config_v2)

    def test_cal_models(self) -> None:
        self.config_v2["outputs"]["cal"]["models"] = {"foo": "bar"}
        with assert_raises_regex(ValueError, "no longer supports models"):
            product_config._validate(self.config_v2)

    def test_simulate_v2(self) -> None:
        self.config_v2["inputs"]["i0_baseline_correlation_products"]["simulate"] = {}
        with assert_raises_regex(ValueError, "simulation is not supported"):
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
        assert_equal(upgraded, self.config)

    def test_upgrade_v3(self) -> None:
        upgraded = product_config._upgrade(self.config)
        assert_equal(upgraded, self.config)

    def test_few_antennas(self) -> None:
        del self.config_v2['inputs']['i0_antenna_channelised_voltage']['antennas'][2:]
        upgraded = product_config._upgrade(self.config_v2)
        del self.config['inputs']['i0_antenna_channelised_voltage']['antennas'][2:]
        del self.config['outputs']['cal']
        del self.config['outputs']['sdp_l1_flags']
        del self.config['outputs']['continuum_image']
        del self.config['outputs']['spectral_image']
        assert_equal(upgraded, self.config)

    def test_unknown_input(self) -> None:
        self.config_v2['inputs']['xyz'] = {
            'type': 'custom',
            'url': 'http://test.invalid/'
        }
        upgraded = product_config._upgrade(self.config_v2)
        assert_equal(upgraded, self.config)


class TestConfiguration(Fixture):
    """Test :class:`~.Configuration`."""

    def setUp(self) -> None:
        super().setUp()
        # Create dummy sensors. Some deliberately have different values to
        # self.config_v2 to ensure that the changes are picked up.
        self.client = fake_katportalclient.KATPortalClient(
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
        # Needed to make the updated config valid
        del self.config['outputs']['l0']['output_channels']
        del self.config['outputs']['beamformer_engineering']['output_channels']
        self.config['outputs']['spectral_image']['output_channels'] = [100, 400]
        patcher = mock.patch('katportalclient.KATPortalClient', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_sim(self) -> None:
        """Test with no sensors required."""
        config = await Configuration.from_config(self.config_sim)
        streams = sorted(config.streams, key=lambda stream: stream.name)
        assert_equal(streams[0].name, 'acv')
        assert_is_instance(config.streams[0], SimAntennaChannelisedVoltageStream)
        assert_equal(streams[1].name, 'bcp')
        assert_is_instance(config.streams[1], SimBaselineCorrelationProductsStream)
        assert_equal(streams[2].name, 'vis')
        assert_is_instance(config.streams[2], VisStream)
        assert_is(streams[2].src_streams[0], config.streams[1])
        assert_is(streams[1].src_streams[0], config.streams[0])

    async def test_cyclic(self) -> None:
        self.config_sim['outputs']['bcp']['src_streams'] = ['vis']
        with assert_raises(ValueError):
            await Configuration.from_config(self.config_sim)

    async def test_bad_stream(self) -> None:
        self.config['outputs']['l0']['continuum_factor'] = 3
        with assert_raises_regex(ValueError, 'Configuration error for stream l0: '):
            await Configuration.from_config(self.config)

    async def test_sensors(self) -> None:
        """Test which needs to apply sensors."""
        config = await Configuration.from_config(self.config)
        bcp = config.by_class(BaselineCorrelationProductsStream)[0]
        assert_equal(bcp.n_chans, 1024)
        assert_equal(bcp.bandwidth, 544e6)
        assert_equal(bcp.antenna_channelised_voltage.band, 'u')
        assert_equal(bcp.int_time, 0.25)

    async def test_connection_failed(self) -> None:
        with mock.patch.object(self.client, 'sensor_subarray_lookup',
                               side_effect=ConnectionRefusedError):
            with assert_raises(product_config.SensorFailure):
                await Configuration.from_config(self.config)

        with mock.patch.object(self.client, 'sensor_values',
                               side_effect=ConnectionRefusedError):
            with assert_raises(product_config.SensorFailure):
                await Configuration.from_config(self.config)

    async def test_sensor_not_found(self):
        del self.client.sensors['cbf_1_i0_baseline_correlation_products_n_bls']
        with assert_raises(product_config.SensorFailure):
            await Configuration.from_config(self.config)

    async def test_sensor_bad_status(self):
        self.client.sensors['cbf_1_i0_baseline_correlation_products_n_bls'] = \
            katportalclient.SensorSample(1234567890.0, 40, 'unreachable')
        with assert_raises(product_config.SensorFailure):
            await Configuration.from_config(self.config)

    async def test_sensor_bad_type(self):
        self.client.sensors['cbf_1_i0_baseline_correlation_products_n_bls'] = \
            katportalclient.SensorSample(1234567890.0, 'not a number', 'nominal')
        with assert_raises(product_config.SensorFailure):
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
        with assert_raises_regex(ValueError, "Only a single band is supported, found 'l', 'u'"):
            await Configuration.from_config(self.config)


def test_stream_classes():
    """Check that each stream in :data:~.STREAM_CLASSES` has the right ``stream_type``."""

    for name, cls in STREAM_CLASSES.items():
        assert_equal(cls.stream_type, name)
