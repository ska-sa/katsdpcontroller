"""Tests for :mod:`katsdpcontroller.product_config`."""

import jsonschema
from nose.tools import assert_equal, assert_in, assert_raises

from .. import product_config


def merge_dicts(*dicts):
    out = {}
    for d in dicts:
        out.update(d)
    return out


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


class TestValidate:
    """Tests for :func:`~katsdpcontroller.product_config.validate`"""

    def setup(self):
        self.config = {
            "version": "1.1",
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
                    "type": "sdp.l0",
                    "src_streams": ["i0_baseline_correlation_products"],
                    "output_int_time": 4.0,
                    "output_channels": [0, 4096],
                    "continuum_factor": 1
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
                }
            },
            "config": {}
        }

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

    def test_v1_0_sdp_cal(self):
        self.config["version"] = "1.0"
        with assert_raises(ValueError):
            product_config.validate(self.config)

    def test_v1_0_simulate_dict(self):
        self.config["inputs"]["i0_baseline_correlation_products"]["simulate"] = {}
        with assert_raises(ValueError):
            product_config.validate(self.config)


class TestConvert:
    """Tests for :func:`~katsdpcontroller.product_config.convert`.

    These tests are fragile because there is more than one valid way to skin
    the cat. They're written by checking what comes out for sanity then
    setting that as the expected value.
    """

    def setup(self):
        self.antennas = ["m000", "m001", "m002", "m003", "m004"]
        self.stream_sources = {
            "cam.http": {"camdata": "http://10.8.67.235/api/client/1"},
            "cbf.antenna_channelised_voltage": {
                "i0.antenna-channelised-voltage": "spead://239.2.1.150+15:7148"
            },
            "cbf.baseline_correlation_products": {
                "i0.baseline-correlation-products": "spead://239.9.3.1+15:7148"
            }, "cbf.tied_array_channelised_voltage": {
                "i0.tied-array-channelised-voltage.0x": "spead://239.9.3.30+15:7148",
                "i0.tied-array-channelised-voltage.0y": "spead://239.9.3.46+15:7148"
            }
        }
        self.expected_inputs = {
            "camdata": {
                "type": "cam.http",
                "url": "http://10.8.67.235/api/client/1"
            },
            "i0_antenna_channelised_voltage": {
                "adc_sample_rate": 1712000000.0,
                "antennas": [
                    "m000",
                    "m001",
                    "m002",
                    "m003",
                    "m004"
                ],
                "bandwidth": 856000000.0,
                "instrument_dev_name": "i0",
                "n_chans": 4096,
                "n_pols": 2,
                "n_samples_between_spectra": 8192,
                "type": "cbf.antenna_channelised_voltage",
                "url": "spead://239.2.1.150+15:7148"
            },
            "i0_baseline_correlation_products": {
                "instrument_dev_name": "i0",
                "int_time": 0.49978856074766354,
                "n_bls": 144,
                "n_chans_per_substream": 256,
                "simulate": False,
                "src_streams": [
                    "i0_antenna_channelised_voltage"
                ],
                "type": "cbf.baseline_correlation_products",
                "url": "spead://239.9.3.1+15:7148",
                "xeng_out_bits_per_sample": 32
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
                "url": "spead://239.9.3.46+15:7148"
            }
        }
        self.expected_l0 = {
            "sdp_l0": {
                "continuum_factor": 1,
                "output_int_time": 2.0,
                "src_streams": [
                    "i0_baseline_correlation_products"
                ],
                "type": "sdp.l0"
            },
            "sdp_l0_continuum": {
                "continuum_factor": 16,
                "output_int_time": 2.0,
                "src_streams": [
                    "i0_baseline_correlation_products"
                ],
                "type": "sdp.l0"
            }
        }
        self.expected_beamformer_ptuse = {
            "beamformer": {
                "src_streams": [
                    "i0_tied_array_channelised_voltage_0x",
                    "i0_tied_array_channelised_voltage_0y"
                ],
                "type": "sdp.beamformer"
            }
        }
        self.expected_beamformer_ssd = {
            "beamformer": {
                "src_streams": [
                    "i0_tied_array_channelised_voltage_0x",
                    "i0_tied_array_channelised_voltage_0y"
                ],
                "store": "ssd",
                "type": "sdp.beamformer_engineering"
            }
        }

    def test_c856M4k(self):
        """No beamformer"""
        config = product_config.convert("c856M4k", self.stream_sources, self.antennas,
                                        0.5, False, False, None)
        expected = {
            "config": {},
            "inputs": self.expected_inputs,
            "outputs": self.expected_l0,
            "version": "1.0"
        }
        assert_equal(expected, config)

    def test_bc856M4k(self):
        """PTUSE beamformer capture"""
        config = product_config.convert("bc856M4k", self.stream_sources, self.antennas,
                                        0.5, False, False, None)
        expected = {
            "config": {},
            "inputs": self.expected_inputs,
            "outputs": merge_dicts(self.expected_l0, self.expected_beamformer_ptuse),
            "version": "1.0"
        }
        assert_equal(expected, config)

    def test_bec856M4kssd(self):
        """HDF5 beamformer capture"""
        config = product_config.convert("bec856M4kssd", self.stream_sources, self.antennas,
                                        0.5, False, False, None)
        expected = {
            "config": {},
            "inputs": self.expected_inputs,
            "outputs": merge_dicts(self.expected_l0, self.expected_beamformer_ssd),
            "version": "1.0"
        }
        assert_equal(expected, config)

    def test_single_pol(self):
        config = product_config.convert("c856M4k1p", self.stream_sources, self.antennas,
                                        0.5, False, False, None)
        self.expected_inputs["i0_antenna_channelised_voltage"]["n_pols"] = 1
        self.expected_inputs["i0_baseline_correlation_products"]["n_bls"] = 40
        expected = {
            "config": {},
            "inputs": self.expected_inputs,
            "outputs": self.expected_l0,
            "version": "1.0"
        }
        assert_equal(expected, config)

    def test_extras(self):
        """Test develop, simulate and wrapper arguments"""
        del self.stream_sources["cam.http"]
        config = product_config.convert("c856M4k", self.stream_sources, self.antennas,
                                        0.5, True, True, "http://invalid.com/wrapper")
        del self.expected_inputs["camdata"]
        self.expected_inputs["i0_baseline_correlation_products"]["simulate"] = True
        self.expected_inputs["i0_tied_array_channelised_voltage_0x"]["simulate"] = True
        self.expected_inputs["i0_tied_array_channelised_voltage_0y"]["simulate"] = True
        expected = {
            "config": {
                "develop": True,
                "wrapper": "http://invalid.com/wrapper"
            },
            "inputs": self.expected_inputs,
            "outputs": self.expected_l0,
            "version": "1.0"
        }
        assert_equal(expected, config)

    def test_bad_graph(self):
        with assert_raises(ValueError):
            product_config.convert("notavalidgraph", self.stream_sources, self.antennas,
                                   0.5, False, False, None)

    def test_two_antenna_channelised_voltage(self):
        acv = self.stream_sources["cbf.antenna_channelised_voltage"]
        acv["i0.bad"] = acv["i0.antenna-channelised-voltage"]
        with assert_raises(ValueError):
            product_config.convert("c856M4k", self.stream_sources, self.antennas,
                                   0.5, False, False, None)

    def test_no_antenna_channelised_voltage(self):
        self.stream_sources["cbf.antenna_channelised_voltage"] = {}
        with assert_raises(ValueError):
            product_config.convert("c856M4k", self.stream_sources, self.antennas,
                                   0.5, False, False, None)
        del self.stream_sources["cbf.antenna_channelised_voltage"]
        with assert_raises(ValueError):
            product_config.convert("c856M4k", self.stream_sources, self.antennas,
                                   0.5, False, False, None)

    def test_bad_output(self):
        with assert_raises(jsonschema.ValidationError):
            product_config.convert("c856M4k", self.stream_sources, self.antennas,
                                   0.5, False, False, "not a valid URL")
