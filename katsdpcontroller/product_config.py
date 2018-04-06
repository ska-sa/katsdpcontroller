"""Support for manipulating product config dictionaries."""

import logging
import itertools
import json
import urllib
import copy

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
        for key, value in overrides.items():
            if value is None:
                new_config.pop(key, None)
            else:
                new_config[key] = override(config.get(key), value)
        return new_config
    return overrides


def _url_n_endpoints(url):
    """Return the number of endpoints in a ``spead://`` URL.

    Parameters
    ----------
    url : str
        URL of the form spead://host[+N]:port

    Raises
    ------
    ValueError
        if `url` is not a valid URL, not a SPEAD url, or is missing a port.
    """
    url_parts = urllib.parse.urlsplit(url)
    if url_parts.scheme != 'spead':
        raise ValueError('non-spead URL {}'.format(url))
    if url_parts.port is None:
        raise ValueError('URL {} has no port'.format(url))
    return len(endpoint_list_parser(None)(url_parts.netloc))


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
    from . import generator     # Imported locally to break circular import

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
        'sdp.vis': ['cbf.baseline_correlation_products'],
        'sdp.beamformer': ['cbf.tied_array_channelised_voltage'],
        'sdp.beamformer_engineering': ['cbf.tied_array_channelised_voltage'],
        'sdp.cal': ['sdp.l0', 'sdp.vis'],
        'sdp.flags': ['sdp.vis']
    }
    for name, stream in itertools.chain(config['inputs'].items(),
                                        config['outputs'].items()):
        src_streams = stream.get('src_streams', [])
        for src in src_streams:
            if src in config['inputs']:
                src_config = config['inputs'][src]
            elif src in config['outputs']:
                src_config = config['outputs'][src]
            else:
                raise ValueError('Unknown source {} in {}'.format(src, name))
            if stream['type'] in src_valid_types:
                valid_types = src_valid_types[stream['type']]
                if src_config['type'] not in valid_types:
                    raise ValueError('Source {} has wrong type for {}'.format(src, name))

    # Can only have one cam.http stream
    cam_http = [name for (name, stream) in config['inputs'].items()
                if stream['type'] == 'cam.http']
    if len(cam_http) > 1:
        raise ValueError('Cannot have more than one cam.http stream')

    input_endpoints = {}
    for name, stream in config['inputs'].items():
        try:
            if stream['type'] in ['cbf.baseline_correlation_products',
                                  'cbf.tied_array_channelised_voltage']:
                n_endpoints = _url_n_endpoints(stream['url'])
                input_endpoints[name] = n_endpoints
                src_stream = stream['src_streams'][0]
                n_chans = config['inputs'][src_stream]['n_chans']
                n_chans_per_substream = stream['n_chans_per_substream']
                if n_chans % n_endpoints != 0:
                    raise ValueError(
                        'n_chans ({}) not a multiple of endpoints ({})'.format(
                            n_chans, n_endpoints))
                n_chans_per_endpoint = n_chans // n_endpoints
                if n_chans_per_endpoint % n_chans_per_substream != 0:
                    raise ValueError(
                        'channels per endpoints ({}) not a multiple of n_chans_per_substream ({})'
                        .format(n_chans_per_endpoint, n_chans_per_substream))
        except ValueError as error:
            raise ValueError('{}: {}'.format(name, error)) from error

    has_flags = set()
    for name, output in config['outputs'].items():
        try:
            # Names of inputs and outputs must be disjoint
            if name in config['inputs']:
                raise ValueError('cannot be both an input and an output')

            # Channel ranges must be non-empty and not overflow
            if output['type'] in ['sdp.l0', 'sdp.vis', 'sdp.beamformer_engineering']:
                if 'output_channels' in output:
                    c = output['output_channels']
                    for src_name in output['src_streams']:
                        src = config['inputs'][src_name]
                        acv = config['inputs'][src['src_streams'][0]]
                        if not 0 <= c[0] < c[1] <= acv['n_chans']:
                            raise ValueError('Channel range {}:{} is invalid'.format(c[0], c[1]))

            if output['type'] in ['sdp.l0', 'sdp.vis']:
                continuum_factor = output['continuum_factor']
                src = config['inputs'][output['src_streams'][0]]
                acv = config['inputs'][src['src_streams'][0]]
                n_chans = acv['n_chans']
                if n_chans % continuum_factor != 0:
                    raise ValueError('n_chans ({}) not a multiple of continuum_factor ({})'.format(
                        n_chans, continuum_factor))
                n_chans //= continuum_factor
                n_ingest = generator.n_ingest_nodes(config, name)
                if n_chans % n_ingest != 0:
                    raise ValueError(
                        'continuum channels ({}) not a multiple of number of ingests ({})'.format(
                            n_chans, n_ingest))

            if output['type'] in ['sdp.flags']:
                calibration = output['calibration'][0]
                if calibration not in config['outputs']:
                    raise ValueError('calibration ({}) does not exist'.format(calibration))
                elif config['outputs'][calibration]['type'] != 'sdp.cal':
                    raise ValueError('calibration ({}) has wrong type {}'
                                     .format(calibration, config['outputs'][calibration]['type']))
                if calibration in has_flags:
                    raise ValueError('calibration ({}) already has a flags output'
                                     .format(calibration))
                if output['src_streams'] != config['outputs'][calibration]['src_streams']:
                    raise ValueError('calibration ({}) has different src_streams'
                                     .format(calibration))
                has_flags.add(calibration)

        except ValueError as error:
            raise ValueError('{}: {}'.format(name, error)) from error


def normalise(config):
    """Convert a config dictionary to a canonical form and return it.

    It is assumed to already have passed :func:`validate`. The following
    changes are made:

    - It is upgraded to the newest version.
    - The following fields are filled in with defaults if not provided:
      - simulate (and True is converted to a dict)
      - excise
      - output_channels
      - parameters, models
      - develop, service_overrides
    """
    def unique_name(prefix, base, current):
        proposed = '{}_{}'.format(prefix, base)
        if proposed not in current:
            return proposed
        seq = 0
        while True:
            proposed = '{}{}_{}'.format(prefix, seq, base)
            if proposed not in current:
                return proposed
            seq += 1

    config = copy.deepcopy(config)
    # Upgrade to 1.1
    if config['version'] == '1.0':
        config['version'] = '1.1'
        # list() because we're mutating the outputs as we go
        for name, output in list(config['outputs'].items()):
            if output['type'] == 'sdp.l0' and output['continuum_factor'] == 1:
                cal = {
                    "type": "sdp.cal",
                    "src_streams": [name]
                }
                config['outputs'][unique_name('cal', name, config['outputs'])] = cal

    # Upgrade to 2.0
    if config['version'] == '1.1':
        config['version'] = '2.0'
        for name, output in list(config['outputs'].items()):
            if output['type'] == 'sdp.l0':
                output['type'] = 'sdp.vis'
                output['archive'] = (output['continuum_factor'] == 1)
            elif output['type'] == 'sdp.cal':
                flags = {
                    "type": "sdp.flags",
                    "src_streams": output['src_streams'],
                    "calibration": [name],
                    "archive": True
                }
                config['outputs'][unique_name('flags', name, config['outputs'])] = flags

    # Fill in defaults
    for name, stream in config['inputs'].items():
        if stream['type'] in ['cbf.baseline_correlation_products',
                              'cbf.tied_array_channelised_voltage']:
            if stream.setdefault('simulate', False) is True:
                stream['simulate'] = {}

    for name, output in config['outputs'].items():
        if output['type'] == 'sdp.vis':
            output.setdefault('excise', True)
        if output['type'] in ['sdp.vis', 'sdp.beamformer_engineering']:
            if 'output_channels' not in output:
                n_chans = None
                for src_name in output['src_streams']:
                    src = config['inputs'][src_name]
                    acv = config['inputs'][src['src_streams'][0]]
                    acv_chans = acv['n_chans']
                    if n_chans is None or acv_chans < n_chans:
                        n_chans = acv_chans
                assert n_chans is not None, "no src_streams found?!"
                output['output_channels'] = [0, n_chans]
        if output['type'] == 'sdp.cal':
            output.setdefault('parameters', {})
            output.setdefault('models', {})

    config['config'].setdefault('develop', False)
    config['config'].setdefault('service_overrides', {})

    validate(config)     # Should never fail if the input was valid
    return config


def parse(config_bytes):
    """Load and validate a config dictionary.

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
