"""Support for manipulating product config dictionaries."""

from __future__ import print_function, division, absolute_import, unicode_literals

import re
import logging
import copy
import itertools
import json

import six
from six.moves import urllib
import jsonschema

from katsdptelstate.endpoint import endpoint_list_parser


logger = logging.getLogger(__name__)
STREAMS_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'type': 'object',
    'additionalProperties': {
        'type': 'object',
        'additionalProperties': {
            'type': 'string',
            'minLength': 1
        }
    }
}
PRODUCT_CONFIG_SCHEMA = json.loads('''
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "definitions": {
        "stream_name": {
            "type": "string",
            "pattern": "^[A-Za-z0-9_]+$"
        },
        "stream_type": {
            "type": "string",
            "pattern": "^[A-Za-z0-9_.]+$"
        },
        "src_streams": {
            "type": "array",
            "items": {"$ref": "#/definitions/stream_name"}
        },
        "nonneg_integer": {
            "type": "integer",
            "minimum": 0
        },
        "positive_integer": {
            "type": "integer",
            "minimum": 1
        },
        "singleton": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1
        },
        "pair": {
            "type": "array",
            "minItems": 2,
            "maxItems": 2
        },
        "channel_range": {
            "$ref": "#/definitions/pair",
            "items": {"$ref": "#/definitions/nonneg_integer"}
        }
    },
    "type": "object",
    "required": ["inputs", "outputs", "config", "version"],
    "additionalProperties": false,
    "properties": {
        "inputs": {
            "type": "object",
            "additionalProperties": false,
            "patternProperties": {
                "^[A-Za-z0-9_]+$": {
                    "type": "object",
                    "required": ["type", "url"],
                    "properties": {
                        "type": {"$ref": "#/definitions/stream_type"},
                        "url": {"type": "string", "format": "uri"},
                        "src_streams": {"$ref": "#/definitions/src_streams"}
                    },
                    "oneOf": [
                        {
                            "properties": {
                                "type": {"enum": ["cbf.antenna_channelised_voltage"]},
                                "antennas": {
                                    "type": "array",
                                    "minItems": 1,
                                    "items": {"type": "string"}
                                },
                                "n_chans": {"$ref": "#/definitions/nonneg_integer"},
                                "n_samples_between_spectra": {"$ref": "#/definitions/nonneg_integer"},
                                "n_pols": {"type": "integer", "enum": [1, 2]},
                                "adc_sample_rate": {"type": "number"},
                                "bandwidth": {"type": "number"},
                                "instrument_dev_name": {"type": "string"}
                            },
                            "required": [
                                "antennas", "n_chans", "n_samples_between_spectra", "n_pols",
                                "adc_sample_rate", "bandwidth", "instrument_dev_name"
                            ]
                        },
                        {
                            "properties": {
                                "type": {"enum": ["cbf.baseline_correlation_products"]},
                                "src_streams": {"$ref": "#/definitions/singleton"},
                                "int_time": {"type": "number"},
                                "n_bls": {"$ref": "#/definitions/nonneg_integer"},
                                "xeng_out_bits_per_sample": {"enum": [32]},
                                "n_chans_per_substream": {"$ref": "#/definitions/nonneg_integer"},
                                "instrument_dev_name": {"type": "string"},
                                "simulate": {"type": "boolean", "default": false}
                            },
                            "required": [
                                "src_streams", "int_time", "n_bls",
                                "xeng_out_bits_per_sample", "n_chans_per_substream",
                                "instrument_dev_name"
                            ]
                        },
                        {
                            "properties": {
                                "type": {"enum": ["cbf.tied_array_channelised_voltage"]},
                                "src_streams": {"$ref": "#/definitions/singleton"},
                                "beng_out_bits_per_sample": {"enum": [8]},
                                "spectra_per_heap": {"$ref": "#/definitions/nonneg_integer"},
                                "n_chans_per_substream": {"$ref": "#/definitions/nonneg_integer"},
                                "instrument_dev_name": {"type": "string"},
                                "simulate": {"type": "boolean", "default": false}
                            },
                            "required": [
                                "src_streams", "beng_out_bits_per_sample", "spectra_per_heap",
                                "n_chans_per_substream", "instrument_dev_name"
                            ]
                        },
                        {
                            "properties": {
                                "type": {"enum": ["cam.http"]}
                            }
                        },
                        {
                            "properties": {
                                "type": {"not": {"enum": [
                                    "cbf.antenna_channelised_voltage",
                                    "cbf.baseline_correlation_products",
                                    "cbf.tied_array_channelised_voltage",
                                    "cam.http"
                                ]}}
                            }
                        }
                    ]
                }
            }
        },
        "outputs": {
            "type": "object",
            "additionalProperties": false,
            "patternProperties": {
                "^[A-Za-z0-9_]+$": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {
                        "type": {"$ref": "#/definitions/stream_type"},
                        "src_streams": {"$ref": "#/definitions/src_streams"}
                    },
                    "additionalProperties": true,
                    "oneOf": [
                        {
                            "properties": {
                                "type": {"enum": ["sdp.l0"]},
                                "src_streams": {"$ref": "#/definitions/singleton"},
                                "output_int_time": {"type": "number"},
                                "output_channels": {"$ref": "#/definitions/channel_range"},
                                "continuum_factor": {"$ref": "#/definitions/positive_integer"},
                                "excise": {"type": "boolean", "default": true},
                                "cal_params": {"type": "object"}
                            },
                            "required": ["src_streams", "output_int_time", "continuum_factor"],
                            "additionalProperties": false
                        },
                        {
                            "properties": {
                                "type": {"enum": ["sdp.beamformer_engineering"]},
                                "src_streams": {"$ref": "#/definitions/pair"},
                                "output_channels": {"$ref": "#/definitions/channel_range"},
                                "store": {
                                    "enum": ["ram", "ssd"]
                                }
                            },
                            "required": ["src_streams", "store"],
                            "additionalProperties": false
                        },
                        {
                            "properties": {
                                "type": {"enum": ["sdp.beamformer"]},
                                "src_streams": {"$ref": "#/definitions/pair"}
                            },
                            "required": ["src_streams"],
                            "additionalProperties": false
                        }
                    ]
                }
            }
        },
        "config": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "develop": {"type": "boolean", "default": false},
                "wrapper": {"type": "string", "format": "uri"}
            }
        },
        "version": {
            "type": "string",
            "enum": ["1.0"]
        }
    }
}''')


def override(config, overrides):
    """Update a config dictionary with overrides, merging dictionaries.

    Where both sides have a dictionary, the override is done recursively.
    Where `overrides` contains a value of `None`, the corresponding entry is
    deleted from the return value.

    .. note:: this does not validate the result, which should be done by the caller.

    Returns
    -------
    dict
        The updated config
    """
    if isinstance(config, dict) and isinstance(overrides, dict):
        new_config = dict(config)
        for key, value in six.iteritems(overrides):
            if value is None:
                new_config.pop(key, None)
            else:
                new_config[key] = override(config.get(key), value)
        return new_config
    else:
        return overrides


def validate(config):
    """Validates a config dict.

    This both validates it against the schema and some semantic contraints.


    Raises
    ------
    jsonschema.ValidationError
        if the config doesn't conform to the schema
    ValueError
        if semantic constraints are violated
    """
    jsonschema.validate(config, PRODUCT_CONFIG_SCHEMA)

    # All stream sources must match known names, and have the right type
    src_valid_types = {
        'cbf.tied_array_channelised_voltage': ['cbf.antenna_channelised_voltage'],
        'cbf.baseline_correlation_products': ['cbf.antenna_channelised_voltage'],
        'cbf.antenna_channelised_voltage': [],
        'cam.http': [],
        'sdp.l0': ['cbf.baseline_correlation_products'],
        'sdp.beamformer': ['cbf.tied_array_channelised_voltage'],
        'sdp.beamformer_engineering': ['cbf.tied_array_channelised_voltage']
    }
    for name, stream in itertools.chain(six.iteritems(config['inputs']),
                                        six.iteritems(config['outputs'])):
        src_streams = stream.get('src_streams', [])
        for src in src_streams:
            if src not in config['inputs']:
                raise ValueError('Unknown source {} in {}'.format(src, name))
            if stream['type'] in src_valid_types:
                valid_types = src_valid_types[stream['type']]
                if config['inputs'][src]['type'] not in valid_types:
                    raise ValueError('Source {} has wrong type for {}'.format(src, name))

    # Can only have one cam.http stream
    cam_http = [name for (name, stream) in six.iteritems(config['inputs'])
                if stream['type'] == 'cam.http']
    if len(cam_http) > 1:
        raise ValueError('Cannot have more than one cam.http stream')

    for name, output in six.iteritems(config['outputs']):
        # Names of inputs and outputs must be disjoint
        if name in config['inputs']:
            raise ValueError('{} cannot be both an input and an output'.format(name))

        # Channel ranges must be non-empty and not overflow
        if output['type'] == 'sdp.l0':
            if 'output_channels' in output:
                c = output['output_channels']
                src = config['inputs'][output['src_streams'][0]]
                acv = config['inputs'][src['src_streams'][0]]
                if not 0 <= c[0] < c[1] <= acv['n_chans']:
                    raise ValueError('Channel range {}:{} for {} is invalid'.format(
                        c[0], c[1], name))


def parse(config_bytes):
    """Parse and validate a config dictionary.

    Raises
    ------
    ValueError
        if `config_bytes` is not valid JSON
    jsonschema.ValidationError
        if the config doesn't conform to the schema
    ValueError
        if semantic constraints are violated
    """
    config = json.loads(config_bytes)
    validate(config)
    return config


def _normalise_name(name):
    return name.replace('-', '_').replace('.', '_')


def convert(graph_name, stream_sources, antennas, dump_rate, simulate, develop, wrapper):
    """Convert the arguments to a legacy ``?data-product-configure`` into a config dictionary.

    Parameters
    ----------
    graph_name : str
        The name of the graph extracted from the subarray product ID e.g. ``c856M4k``
    stream_sources : dict
        Dictionary of dictionaries, with the outer key being stream type, inner
        key being stream name, and value being URLs.
    antennas : list of str
        Names of the antennas to capture
    dump_rate : float
        Requested output rate for L0 stream, in dumps per second
    simulate : bool
        Whether to use a simulator to produce the CBF input streams
    develop : bool
        Development mode (relaxes some constraints)
    wrapper : str or ``None``
        Wrapper script to run in every container
    """

    jsonschema.validate(stream_sources, STREAMS_SCHEMA)

    def expect_n_in(type_, number):
        if type_ not in stream_sources:
            raise ValueError('No {} streams provided'.format(type_))
        if len(stream_sources[type_]) != number:
            raise ValueError('Expected {} {} streams, {} found'.format(
                number, type_, len(stream_sources[type_])))

    expect_n_in('cbf.antenna_channelised_voltage', 1)
    expect_n_in('cbf.baseline_correlation_products', 1)

    # Parse the graph name to figure out what outputs to produce
    match = re.match('^(?P<mode>c|bc)856M(?P<channels>4|32)k(?P<single_pol>1p|)$', graph_name)
    if not match:
        match = re.match('^bec856M(?P<channels>4|32)k(?P<mode>ssd|ram)(?P<single_pol>1p|)$',
                         graph_name)
        if not match:
            raise ValueError('Unsupported graph ' + graph_name)
    beamformer_mode = match.group('mode')
    cbf_pols = 1 if match.group('single_pol') else 2
    cbf_channels = int(match.group('channels')) * 1024

    # CBF has only power-of-two antenna counts, minimum 4.
    # We work in inputs rather than antennas because single-pol instruments
    # are a hack that treats a pair of single-input antennas like a
    # dual-input antenna for the purposes of baseline ordering.
    cbf_inputs = 8
    while cbf_inputs < len(antennas) * cbf_pols:
        cbf_inputs *= 2
    cbf_bls = cbf_inputs * (cbf_inputs // 2 + 1)
    inputs = {}
    for type_, streams in stream_sources.items():
        for name, url in streams.items():
            cur = {
                'type': type_,
                'url': url
            }
            url_parts = urllib.parse.urlsplit(url)
            if url_parts.scheme == 'spead':
                endpoints = endpoint_list_parser(None)(url_parts.netloc)
            else:
                endpoints = []
            if type_.startswith('cbf.'):
                cur['instrument_dev_name'] = name.split('.', 1)[0]
            if type_ in ['cbf.baseline_correlation_products',
                         'cbf.tied_array_channelised_voltage']:
                cur['src_streams'] = [
                    _normalise_name(src_name)
                    for src_name in stream_sources['cbf.antenna_channelised_voltage']
                ]
                cur['n_chans_per_substream'] = cbf_channels // len(endpoints)
                cur['simulate'] = simulate
            if type_ == 'cbf.antenna_channelised_voltage':
                cur['antennas'] = antennas
                cur['n_chans'] = cbf_channels
                cur['n_pols'] = cbf_pols
                # Note: assumes wideband
                cur['n_samples_between_spectra'] = 2 * cbf_channels
                # Note: hard-coded for L band
                cur['adc_sample_rate'] = 1712000000.0
                cur['bandwidth'] = 856000000.0
            elif type_ == 'cbf.baseline_correlation_products':
                # Note: hard-coded for L band and standard correlator config
                cur['int_time'] = 0.49978856074766354
                cur['n_bls'] = cbf_bls
                # Note: assumes one endpoint == one substream
                cur['xeng_out_bits_per_sample'] = 32
            elif type_ == 'cbf.tied_array_channelised_voltage':
                cur['spectra_per_heap'] = 256
                cur['beng_out_bits_per_sample'] = 8
            elif type_ == 'cam.http':
                pass      # No further configuration needed
            else:
                logger.info('Ignoring input stream %s of unknown type %s',
                            name, type_)
            inputs[_normalise_name(name)] = cur

    outputs = {}
    # Create L0 visibility output
    outputs['sdp_l0'] = {
        'type': 'sdp.l0',
        'src_streams': [_normalise_name(name)
                        for name in stream_sources['cbf.baseline_correlation_products']],
        'output_int_time': 1.0 / dump_rate,
        'continuum_factor': 1,
    }
    outputs['sdp_l0_continuum'] = copy.deepcopy(outputs['sdp_l0'])
    outputs['sdp_l0_continuum']['continuum_factor'] = 16

    # Create beamformer output, if requested
    if beamformer_mode != 'c':
        expect_n_in('cbf.tied_array_channelised_voltage', 2)
        if beamformer_mode == 'bc':
            outputs['beamformer'] = {
                'type': 'sdp.beamformer'
            }
        else:
            outputs['beamformer'] = {
                'type': 'sdp.beamformer_engineering',
                'store': beamformer_mode
            }
        # Sort the streams to ensure 0x is before 0y
        beamformer_names = [_normalise_name(name)
                            for name in stream_sources['cbf.tied_array_channelised_voltage']]
        outputs['beamformer']['src_streams'] = sorted(beamformer_names)

    config = {}
    if develop:
        config['develop'] = True
    if wrapper is not None:
        config['wrapper'] = wrapper

    ret = {'inputs': inputs, 'outputs': outputs, 'config': config, 'version': '1.0'}
    try:
        validate(ret)
    except Exception as error:
        logger.error("Generated config %s is invalid (source %s)",
                     json.dumps(ret, indent=2, sort_keys=True),
                     json.dumps(stream_sources, indent=2, sort_keys=True),
                     exc_info=True)
        raise
    return ret
