"""Tests for :mod:`katsdpcontroller.product_config`."""

import copy
import logging
from unittest import mock
from typing import Dict, Optional, Any

import asynctest
import jsonschema
import yarl
import katpoint
import katportalclient
from nose.tools import (
    assert_equal, assert_almost_equal, assert_in, assert_is, assert_is_none,
    assert_true, assert_false, assert_raises, assert_raises_regex, assert_logs
)

from .. import product_config
from ..product_config import (
    AntennaChannelisedVoltageStream,
    SimAntennaChannelisedVoltageStream,
    BaselineCorrelationProductsStream,
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
    Configuration
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


class TestAntennaChannelisedVoltageStream:
    """Test :class:`~.AntennaChannelisedVoltageStream`."""

    def test_from_config(self) -> None:
        config = {
            'type': 'cbf.antenna_channelised_voltage',
            'url': 'spead://239.0.0.0+7:7148',
            'antennas': ['m000', 'm001'],
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
        assert_equal(acv.antennas, ['m000', 'm001'])
        assert_equal(acv.band, 'l')
        assert_equal(acv.n_channels, 32768)
        assert_equal(acv.bandwidth, 107e6)
        assert_equal(acv.centre_frequency, 1284e6)
        assert_equal(acv.adc_sample_rate, 1712e6)
        assert_equal(acv.n_samples_between_spectra, 524288)
        assert_equal(acv.instrument_dev_name, 'narrow1')


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
        assert_equal(acv.n_channels, 32768)
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


def make_antenna_channelised_voltage(antennas=('m000', 'm002')) -> AntennaChannelisedVoltageStream:
    return AntennaChannelisedVoltageStream(
        'narrow1_acv', [],
        url=yarl.URL('spead2://239.0.0.0+7:7148'),
        antennas=antennas,
        band='l',
        n_channels=32768,
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
        n_channels=512
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
        assert_equal(bcp.antennas, ['m000', 'm002'])
        assert_equal(bcp.n_channels, 32768)
        assert_equal(bcp.n_channels_per_endpoint, 4096)
        assert_equal(bcp.n_substreams, 16)
        assert_equal(bcp.n_antennas, 2)
        assert_equal(bcp.bandwidth, 107e6)
        assert_equal(bcp.centre_frequency, 1284e6)
        assert_equal(bcp.adc_sample_rate, 1712e6)
        assert_equal(bcp.n_samples_between_spectra, 524288)
        assert_equal(bcp.net_bandwidth(1.0, 0), 40 * 32768 * 8 * 2 * 8)

    def test_bad_endpoint_count(self) -> None:
        self.config['url'] = 'spead://239.1.0.0+8:7148'
        with assert_raises_regex(ValueError,
                                 r'n_channels \(32768\) is not a multiple of endpoints \(9\)'):
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


class TestSimBaselineCorrelationProductsStream:
    """Test :class:`~.SimBaselineCorrelationProductsStream`."""

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
        # Most properties are assumed to be tested via
        # TestBaselineCorrelationProductsStream and are not re-tested here.
        assert_equal(bcp.n_channels_per_substream, 8)
        assert_equal(bcp.n_substreams, 64)
        # Check that int_time is rounded to nearest multiple of 512
        assert_equal(bcp.int_time, 1024.0)

    def test_defaults(self):
        del self.config['n_chans_per_substream']
        bcp = SimBaselineCorrelationProductsStream.from_config(
            Options(), 'narrow2_bcp', self.config, [self.acv], {}
        )
        assert_equal(bcp.n_channels_per_substream, 32)
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
        assert_equal(tacv.n_channels_per_substream, 64)
        assert_equal(tacv.spectra_per_heap, 256)
        assert_equal(tacv.instrument_dev_name, 'beam')
        assert_equal(tacv.size, 32768 * 256 * 2 * 2)
        assert_is(tacv.antenna_channelised_voltage, self.acv)
        assert_equal(tacv.bandwidth, 107e6)
        assert_equal(tacv.antennas, ['m000', 'm002'])
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
        assert_equal(tacv.n_channels_per_substream, 4)
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
        assert_equal(tacv.spectra_per_heap, product_config.KATCBFSIM_SPECTRA_PER_HEAP)
        assert_equal(tacv.n_channels_per_substream, 32)


def make_baseline_correlation_products(
        antenna_channelised_voltage: Optional[AntennaChannelisedVoltageStream] = None
        ) -> BaselineCorrelationProductsStream:
    if antenna_channelised_voltage is None:
        antenna_channelised_voltage = make_antenna_channelised_voltage()
    return BaselineCorrelationProductsStream(
        'narrow1_bcp', [antenna_channelised_voltage],
        url=yarl.URL('spead://239.2.0.0+63:7148'),
        int_time=0.5,
        n_channels_per_substream=512,
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
        n_channels_per_substream=128,
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
        assert_equal(vis.n_channels, 1984)
        assert_equal(vis.n_spectral_channels, 3968)
        assert_equal(vis.antennas, ['m000', 'm002'])
        assert_equal(vis.n_antennas, 2)
        assert_equal(vis.n_pols, 2)
        assert_equal(vis.n_baselines, 12)
        assert_equal(vis.size, 1984 * (12 * 10 + 4))
        assert_equal(vis.flag_size, 1984 * 12)
        assert_equal(vis.net_bandwidth(1.0, 0), vis.size / vis.int_time * 8)
        assert_equal(vis.flag_bandwidth(1.0, 0), vis.flag_size / vis.int_time * 8)

    def test_defaults(self) -> None:
        del self.config['output_channels']
        del self.config['excise']
        vis = VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        assert_equal(vis.output_channels, (0, 32768))
        assert_equal(vis.excise, True)

    def test_bad_continuum_factor(self) -> None:
        self.config['continuum_factor'] = 3
        with assert_raises_regex(
                ValueError,
                r'CBF channels \(32768\) is not a multiple of continuum_factor \(3\)'):
            VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})
        self.config['continuum_factor'] = 1024
        with assert_raises_regex(
                ValueError,
                r'Channel range \(128:4096\) is not a multiple of continuum_factor \(1024\)'):
            VisStream.from_config(Options(), 'sdp_l0', self.config, [self.bcp], {})

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
        assert_equal(bf.n_channels, 32768)

    def test_mismatched_sources(self) -> None:
        acv = make_antenna_channelised_voltage()
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
        assert_equal(bf.n_channels, 896)

    def test_defaults(self) -> None:
        del self.config['output_channels']
        bf = BeamformerEngineeringStream.from_config(
            Options(), 'beamformer', self.config, self.tacv, {}
        )
        assert_equal(bf.output_channels, (0, 32768))
        assert_equal(bf.n_channels, 32768)

    def test_misaligned_channels(self) -> None:
        self.config['output_channels'] = [1, 2]
        with assert_raises_regex(ValueError,
                                 r'Channel range \(1:2\) is not aligned to the multicast streams'):
            BeamformerEngineeringStream.from_config(
                Options(), 'beamformer', self.config, self.tacv, {}
            )


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
    acv = make_antenna_channelised_voltage(['m000', 'm001', 'm002', 'm003'])
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
        assert_equal(cal.buffer_time, product_config.DEFAULT_CAL_BUFFER_TIME)
        assert_equal(cal.max_scans, product_config.DEFAULT_CAL_MAX_SCANS)

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
        buffer_time=product_config.DEFAULT_CAL_BUFFER_TIME,
        max_scans=product_config.DEFAULT_CAL_MAX_SCANS
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
        assert_equal(flags.n_channels, self.vis.n_channels)
        assert_equal(flags.n_baselines, self.vis.n_baselines)
        assert_equal(flags.n_vis, self.vis.n_vis)
        assert_equal(flags.size, self.vis.flag_size)
        assert_equal(flags.net_bandwidth(), self.vis.flag_bandwidth())
        assert_equal(flags.int_time, self.vis.int_time)

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


class Fixture(asynctest.TestCase):
    """Base class providing some sample config dicts"""

    def setUp(self):
        self.config = {
            "version": "2.6",
            "inputs": {
                "camdata": {
                    "type": "cam.http",
                    "url": "http://10.8.67.235/api/client/1"
                },
                "i0_antenna_channelised_voltage": {
                    "type": "cbf.antenna_channelised_voltage",
                    "url": "spead://239.2.1.150+15:7148",
                    "antennas": ["m000", "m001", "m003", "m063"],
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


class TestValidate(Fixture):
    """Tests for :func:`~katsdpcontroller.product_config.validate`"""

    def test_good(self):
        product_config.validate(self.config)

    def test_bad_version(self):
        self.config["version"] = "1.10"
        with assert_raises(jsonschema.ValidationError):
            product_config.validate(self.config)

    def test_input_bad_property(self):
        """Test that the error message on an invalid input is sensible"""
        del self.config["inputs"]["i0_antenna_channelised_voltage"]["n_pols"]
        with assert_raises(jsonschema.ValidationError) as cm:
            product_config.validate(self.config)
        assert_in("'n_pols' is a required property", str(cm.exception))

    def test_output_bad_property(self):
        """Test that the error message on an invalid output is sensible"""
        del self.config["outputs"]["l0"]["continuum_factor"]
        with assert_raises(jsonschema.ValidationError) as cm:
            product_config.validate(self.config)
        assert_in("'continuum_factor' is a required property", str(cm.exception))

    def test_input_missing_stream(self):
        """An input whose ``src_streams`` reference does not exist"""
        del self.config["inputs"]["i0_antenna_channelised_voltage"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("Unknown source i0_antenna_channelised_voltage", str(cm.exception))

    def test_input_not_spead(self):
        """A CBF stream has a non-spead URL scheme"""
        self.config["inputs"]["i0_baseline_correlation_products"]["url"] = "http://dummy/"
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("non-spead URL", str(cm.exception))

    def test_input_no_spead_port(self):
        """A CBF stream has a spead URL but with no port"""
        self.config["inputs"]["i0_baseline_correlation_products"]["url"] = "spead://239.9.3.1+15"
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("has no port", str(cm.exception))

    def test_input_bad_n_endpoints(self):
        """Number of endpoints doesn't divide into number of channels"""
        self.config["inputs"]["i0_baseline_correlation_products"]["url"] = \
            "spead://239.9.3.1+14:7148"
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("not a multiple of endpoints", str(cm.exception))

    def test_output_missing_stream(self):
        """An output whose ``src_streams`` reference does not exist"""
        del self.config["inputs"]["i0_baseline_correlation_products"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("Unknown source i0_baseline_correlation_products", str(cm.exception))

    def test_stream_wrong_type(self):
        """An entry in ``src_streams`` refers to the wrong type"""
        self.config["outputs"]["l0"]["src_streams"] = ["i0_antenna_channelised_voltage"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("has wrong type", str(cm.exception))

    def test_stream_name_conflict(self):
        """An input and an output have the same name"""
        self.config["outputs"]["i0_antenna_channelised_voltage"] = self.config["outputs"]["l0"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("cannot be both an input and an output", str(cm.exception))

    def test_bad_channel_range(self):
        self.config["outputs"]["l0"]["output_channels"] = [10, 10]  # Empty range
        with assert_raises(ValueError):
            product_config.validate(self.config)
        self.config["outputs"]["l0"]["output_channels"] = [10, 4097]   # Overflows
        with assert_raises(ValueError):
            product_config.validate(self.config)

    def test_bad_continuum_factor(self):
        self.config["outputs"]["l0"]["continuum_factor"] = 3
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("not a multiple of continuum_factor", str(cm.exception))

    def test_too_few_continuum_channels(self):
        self.config["outputs"]["l0"]["continuum_factor"] = 4096
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("not a multiple of number of ingests", str(cm.exception))

    def test_multiple_cam_http(self):
        self.config["inputs"]["camdata2"] = self.config["inputs"]["camdata"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("have more than one cam.http", str(cm.exception))

    def test_bad_n_chans_per_substream(self):
        self.config["inputs"]["i0_baseline_correlation_products"]["n_chans_per_substream"] = 123
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("not a multiple of", str(cm.exception))

    def test_mismatched_beamformer_pols(self):
        self.config["inputs"]["myacv"] = self.config["inputs"]["i0_antenna_channelised_voltage"]
        self.config["inputs"]["i0_tied_array_channelised_voltage_0y"]["src_streams"] = ["myacv"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("streams do not come from the same channeliser", str(cm.exception))

    def test_v2_1_multiple_flags_per_cal(self):
        self.config["version"] = "2.1"
        self.config["outputs"]["flags2"] = {
            "type": "sdp.flags",
            "src_streams": ["l0"],
            "calibration": ["cal"],
            "archive": True
        }
        # Remove outputs not valid in 2.1
        del self.config["outputs"]["continuum_image"]
        del self.config["outputs"]["spectral_image"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("already has a flags output", str(cm.exception))

    def test_calibration_does_not_exist(self):
        self.config["outputs"]["sdp_l1_flags"]["calibration"] = ["bad"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("does not exist", str(cm.exception))

    def test_calibration_wrong_type(self):
        self.config["outputs"]["sdp_l1_flags"]["calibration"] = ["l0"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("has wrong type", str(cm.exception))

    def test_flags_mismatched_src_streams(self):
        self.config["outputs"]["another_l0"] = copy.copy(self.config["outputs"]["l0"])
        self.config["outputs"]["another_l0"]["output_int_time"] = 5.0
        self.config["outputs"]["sdp_l1_flags"]["src_streams"] = ["another_l0"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("does not match", str(cm.exception))

    def test_flags_bad_continuum_factor(self):
        self.config["outputs"]["another_l0"] = copy.copy(self.config["outputs"]["l0"])
        self.config["outputs"]["l0"]["continuum_factor"] = 2
        self.config["outputs"]["sdp_l1_flags"]["src_streams"] = ["another_l0"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("bad continuum_factor", str(cm.exception))

    def test_spectral_image_output_channels(self):
        self.config["outputs"]["l0"]["continuum_factor"] = 2
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("Channel range 100:4000 is invalid", str(cm.exception))

        del self.config["outputs"]["l0"]["output_channels"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("Channel range 100:4000 is invalid", str(cm.exception))

    def test_v2_1_flags_wrong_src_streams(self):
        self.config["version"] = "2.1"
        self.config["outputs"]["another_l0"] = self.config["outputs"]["l0"]
        self.config["outputs"]["sdp_l1_flags"]["src_streams"] = ["another_l0"]
        # Remove outputs not valid in 2.1
        del self.config["outputs"]["continuum_image"]
        del self.config["outputs"]["spectral_image"]
        with assert_raises(ValueError) as cm:
            product_config.validate(self.config)
        assert_in("has different src_streams", str(cm.exception))


class TestUpdateFromSensors(Fixture):
    """Tests for :func:`~katsdpcontroller.product_config.update_from_sensors`"""
    def setUp(self):
        super().setUp()
        # Create dummy sensors. Some deliberately have different values to
        # self.config to ensure that the changes are picked up.
        self.client = fake_katportalclient.KATPortalClient(
            components={'cbf': 'cbf_1', 'sub': 'subarray_1'},
            sensors={
                'cbf_1_i0_antenna_channelised_voltage_n_chans': 1024,
                'cbf_1_i0_adc_sample_rate': 1088e6,
                'cbf_1_i0_antenna_channelised_voltage_n_samples_between_spectra': 2048,
                'subarray_1_streams_i0_antenna_channelised_voltage_bandwidth': 544e6,
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

    async def test_basic(self):
        with assert_logs(product_config.logger, logging.WARNING):
            config = await product_config.update_from_sensors(self.config)
        acv = config['inputs']['i0_antenna_channelised_voltage']
        assert_equal(acv['n_chans'], 1024)
        assert_equal(acv['adc_sample_rate'], 1088e6)
        assert_equal(acv['bandwidth'], 544e6)
        assert_equal(acv['n_samples_between_spectra'], 2048)
        bcp = config['inputs']['i0_baseline_correlation_products']
        assert_equal(bcp['int_time'], 0.25)
        assert_equal(bcp['n_bls'], 40)
        assert_equal(bcp['xeng_out_bits_per_sample'], 32)
        assert_equal(bcp['n_chans_per_substream'], 64)
        beam_x = config['inputs']['i0_tied_array_channelised_voltage_0x']
        assert_equal(beam_x['beng_out_bits_per_sample'], 8)
        assert_equal(beam_x['n_chans_per_substream'], 64)
        assert_equal(beam_x['spectra_per_heap'], 256)
        # Ensure that the original was not modified
        assert_equal(self.config['inputs']['i0_antenna_channelised_voltage']['n_chans'], 4096)

    async def test_no_cam_http(self):
        del self.config['inputs']['camdata']
        config = await product_config.update_from_sensors(self.config)
        # Ensure that nothing happened
        assert_equal(config['inputs']['i0_antenna_channelised_voltage']['n_chans'], 4096)

    async def test_simulate(self):
        self.config['inputs']['i0_baseline_correlation_products']['simulate'] = {}
        # Needed to make the config valid after i0_antenna_channelised_voltage is overridden
        self.config['inputs']['i0_baseline_correlation_products']['n_chans_per_substream'] = 64
        config = await product_config.update_from_sensors(self.config)
        # Ensure that nothing happened on i0_baseline_correlation_products
        assert_equal(config['inputs']['i0_baseline_correlation_products']['int_time'], 0.499)

    async def test_connection_failed(self):
        with mock.patch.object(self.client, 'sensor_subarray_lookup',
                               side_effect=ConnectionRefusedError):
            with assert_raises(product_config.SensorFailure):
                await product_config.update_from_sensors(self.config)

        with mock.patch.object(self.client, 'sensor_value',
                               side_effect=ConnectionRefusedError):
            with assert_raises(product_config.SensorFailure):
                await product_config.update_from_sensors(self.config)

    async def test_sensor_not_found(self):
        del self.client.sensors['cbf_1_i0_baseline_correlation_products_n_bls']
        with assert_raises(product_config.SensorFailure):
            await product_config.update_from_sensors(self.config)

    async def test_bad_status(self):
        self.client.sensors['cbf_1_i0_baseline_correlation_products_n_bls'] = \
            katportalclient.SensorSample(1234567890.0, 40, 'unreachable')
        with assert_raises(product_config.SensorFailure):
            await product_config.update_from_sensors(self.config)

    async def test_bad_schema(self):
        self.client.sensors['cbf_1_i0_baseline_correlation_products_n_bls'] = \
            katportalclient.SensorSample(1234567890.0, 'not a number', 'nominal')
        with assert_raises(product_config.SensorFailure):
            await product_config.update_from_sensors(self.config)


class TestNormalise(Fixture):
    """Tests for :func:`~katsdpcontroller.product_config.normalise`"""

    def test_latest(self):
        # Adjust some things to get full test coverage
        expected = copy.deepcopy(self.config)
        self.config["inputs"]["i0_baseline_correlation_products"]["simulate"] = True
        del self.config["outputs"]["l0"]["output_channels"]
        del self.config["outputs"]["beamformer_engineering"]["output_channels"]
        del self.config["outputs"]["continuum_image"]["uvblavg_parameters"]
        del self.config["outputs"]["continuum_image"]["mfimage_parameters"]
        del self.config["outputs"]["spectral_image"]["output_channels"]

        expected["inputs"]["i0_baseline_correlation_products"]["simulate"] = {
            "clock_ratio": 1.0,
            "sources": []
        }
        expected["inputs"]["i0_tied_array_channelised_voltage_0x"]["simulate"] = False
        expected["inputs"]["i0_tied_array_channelised_voltage_0y"]["simulate"] = False
        expected["outputs"]["l0"]["excise"] = True
        expected["outputs"]["l0"]["output_channels"] = [0, 4096]
        expected["outputs"]["beamformer_engineering"]["output_channels"] = [0, 4096]
        expected["outputs"]["cal"]["parameters"] = {}
        expected["outputs"]["cal"]["models"] = {}
        expected["outputs"]["spectral_image"]["output_channels"] = [0, 4096]
        expected["outputs"]["spectral_image"]["parameters"] = {}
        expected["config"]["develop"] = False
        expected["config"]["service_overrides"] = {}

        config = product_config.normalise(self.config)
        # Uncomment to debug differences
        # import json
        # with open('actual.json', 'w') as f:
        #     json.dump(config, f, indent=2, sort_keys=True)
        # with open('expected.json', 'w') as f:
        #     json.dump(expected, f, indent=2, sort_keys=True)
        assert_equal(config, expected)
