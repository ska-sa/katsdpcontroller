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

from . import schemas


logger = logging.getLogger(__name__)


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

    This validates it against both the schema and some semantic contraints.

    Raises
    ------
    jsonschema.ValidationError
        if the config doesn't conform to the schema
    ValueError
        if semantic constraints are violated
    """
    # Error messages for the oneOf parts of the schema are not helpful by
    # default, because it doesn't know which branch is the relevant one. The
    # "not" branches are generally just there to make validation conditional
    # on the type.
    def relevance(error):
        return (error.validator == 'not',) + jsonschema.exceptions.relevance(error)

    errors = schemas.PRODUCT_CONFIG.iter_errors(config)
    error = jsonschema.exceptions.best_match(errors, key=relevance)
    if error is not None:
        raise error

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

    input_endpoints = {}
    for name, stream in six.iteritems(config['inputs']):
        if stream['type'] in ['cbf.baseline_correlation_products',
                              'cbf.tied_array_channelised_voltage']:
            url_parts = urllib.parse.urlsplit(stream['url'])
            if url_parts.scheme != 'spead':
                raise ValueError('{}: non-spead URL {}'.format(name, stream['url']))
            if url_parts.port is None:
                raise ValueError('{}: URL {} has no port'.format(name, stream['url']))
            n_endpoints = len(endpoint_list_parser(None)(url_parts.netloc))
            input_endpoints[name] = n_endpoints
            src_stream = stream['src_streams'][0]
            n_chans = config['inputs'][src_stream]['n_chans']
            n_chans_per_substream = stream['n_chans_per_substream']
            if n_chans % n_endpoints != 0:
                raise ValueError(
                    '{}: n_chans ({}) not a multiple of endpoints ({})'.format(
                        name, n_chans, n_endpoints))
            n_chans_per_endpoint = n_chans // n_endpoints
            if n_chans_per_endpoint % n_chans_per_substream != 0:
                raise ValueError(
                    '{}: channels per endpoints ({}) not a multiple of n_chans_per_substream ({})'
                    .format(name, n_chans_per_endpoint, n_chans_per_substream))

    for name, output in six.iteritems(config['outputs']):
        # Names of inputs and outputs must be disjoint
        if name in config['inputs']:
            raise ValueError('{} cannot be both an input and an output'.format(name))

        # Channel ranges must be non-empty and not overflow
        if output['type'] in ['sdp.l0', 'sdp.beamformer_engineering']:
            if 'output_channels' in output:
                c = output['output_channels']
                for src_name in output['src_streams']:
                    src = config['inputs'][src_name]
                    acv = config['inputs'][src['src_streams'][0]]
                    if not 0 <= c[0] < c[1] <= acv['n_chans']:
                        raise ValueError('Channel range {}:{} for {} is invalid'.format(
                            c[0], c[1], name))
                    # beamformer_engineering requires alignment to multicast addresses
                    if output['type'] == 'sdp.beamformer_engineering':
                        n_endpoints = input_endpoints[src_name]
                        n_chans_per_endpoint = acv['n_chans'] // n_endpoints
                        if c[0] % n_chans_per_endpoint != 0 or c[1] % n_chans_per_endpoint != 0:
                            raise ValueError(
                                'Channel range {}:{} for {} is not aligned to endpoints'.format(
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

    schemas.STREAMS.validate(stream_sources)

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
                # simulation not yet supported for tied_array_channelised_voltage
                cur['simulate'] = simulate and type_ != 'cbf.tied_array_channelised_voltage'
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
