"""Tests for :mod:`katsdpcontroller.product_config`."""

import jsonschema
import copy
import logging
from unittest import mock
import asynctest

import katportalclient
from nose.tools import assert_equal, assert_in, assert_raises, assert_logs

from .. import product_config
from . import fake_katportalclient


class TestRecursiveDiff:
    def test_base_add(self):
        out = product_config._recursive_diff({'a': 1}, {'a': 1, 'b': 2})
        assert_equal(out, 'b added')

    def test_base_remove(self):
        out = product_config._recursive_diff({'a': 1, 'b': 2}, {'a': 1})
        assert_equal(out, 'b removed')

    def test_base_change(self):
        out = product_config._recursive_diff({'a': 1, 'b': 2}, {'a': 1, 'b': 3})
        assert_equal(out, 'b changed from 2 to 3')

    def test_nested_add(self):
        out = product_config._recursive_diff({'x': {}}, {'x': {'a': 1}})
        assert_equal(out, 'x.a added')

    def test_nested_remove(self):
        out = product_config._recursive_diff({'x': {'a': 1}}, {'x': {}})
        assert_equal(out, 'x.a removed')

    def test_nested_change(self):
        out = product_config._recursive_diff({'x': {'a': 1, 'b': 2}}, {'x': {'a': 1, 'b': 3}})
        assert_equal(out, 'x.b changed from 2 to 3')


class TestOverride:
    """Tests for :func:`~katsdpcontroller.product_config.override`"""

    def test_add(self):
        out = product_config.override({"a": 1}, {"b": 2})
        assert_equal({"a": 1, "b": 2}, out)

    def test_remove(self):
        out = product_config.override({"a": 1, "b": 2}, {"b": None})
        assert_equal({"a": 1}, out)
        out = product_config.override(out, {"b": None})  # Already absent
        assert_equal({"a": 1}, out)

    def test_replace(self):
        out = product_config.override({"a": 1, "b": 2}, {"b": 3})
        assert_equal({"a": 1, "b": 3}, out)

    def test_recurse(self):
        orig = {"a": {"aa": 1, "ab": 2}, "b": {"ba": {"c": 10}, "bb": [5]}}
        override = {"a": {"aa": [], "ab": None, "ac": 3}, "b": {"bb": [1, 2]}}
        out = product_config.override(orig, override)
        assert_equal({"a": {"aa": [], "ac": 3}, "b": {"ba": {"c": 10}, "bb": [1, 2]}}, out)


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
        self.config_v1_0 = copy.deepcopy(self.config)
        self.config_v1_0["version"] = "1.0"
        self.config_v1_0["outputs"]["l0"]["type"] = "sdp.l0"
        del self.config_v1_0["outputs"]["cal"]
        del self.config_v1_0["outputs"]["sdp_l1_flags"]
        del self.config_v1_0["outputs"]["continuum_image"]
        del self.config_v1_0["outputs"]["spectral_image"]
        del self.config_v1_0["outputs"]["l0"]["archive"]


class TestValidate(Fixture):
    """Tests for :func:`~katsdpcontroller.product_config.validate`"""

    def test_good(self):
        product_config.validate(self.config)
        product_config.validate(self.config_v1_0)

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

    def test_v1_0_sdp_cal(self):
        self.config["version"] = "1.0"
        with assert_raises(jsonschema.ValidationError):
            product_config.validate(self.config)

    def test_v1_0_simulate_dict(self):
        self.config_v1_0["inputs"]["i0_baseline_correlation_products"]["simulate"] = {}
        with assert_raises(jsonschema.ValidationError):
            product_config.validate(self.config_v1_0)


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

    def test_v1_0(self):
        # Adjust some things to get full test coverage
        del self.config_v1_0["outputs"]["l0"]["output_channels"]
        del self.config_v1_0["outputs"]["beamformer_engineering"]["output_channels"]
        self.config_v1_0["inputs"]["i0_baseline_correlation_products"]["simulate"] = True
        config = product_config.normalise(self.config_v1_0)
        expected = self.config
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
        expected["config"]["develop"] = False
        expected["config"]["service_overrides"] = {}
        del expected["outputs"]["continuum_image"]
        del expected["outputs"]["spectral_image"]
        assert_equal(config, expected)

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

    def test_name_conflict(self):
        self.config_v1_0["outputs"]["cal"] = self.config_v1_0["outputs"]["l0"]
        config = product_config.normalise(self.config_v1_0)
        assert_in("cal0", config["outputs"])
        assert_equal("sdp.vis", config["outputs"]["cal"]["type"])
        assert_equal("sdp.cal", config["outputs"]["cal0"]["type"])
