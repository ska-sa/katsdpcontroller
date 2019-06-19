"""Support for manipulating product config dictionaries."""

import logging
import itertools
import json
import urllib
import copy
from abc import ABC, abstractmethod
from distutils.version import StrictVersion

import jsonschema

from katsdptelstate.endpoint import endpoint_list_parser
import katportalclient

from . import schemas


logger = logging.getLogger(__name__)


class _Sensor(ABC):
    """A sensor that provides some data about an input stream.

    This is an abstract base class. Derived classes implement
    :meth:`full_name` to map the base name to the system-wide
    sensor name to query from katportal.
    """
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def full_name(self, components, stream):
        """Obtain system-wide name for the sensor.

        Parameters
        ----------
        components: Mapping[str, str]
            Maps logical component name (e.g. 'cbf') to actual component
            name (e.g. 'cbf_1').
        stream : str
            Name of the input stream (including instrument prefix)
        """
        pass


class _CBFSensor(_Sensor):
    def full_name(self, components, stream):
        return '{}_{}_{}'.format(components['cbf'], stream, self.name)


class _SubSensor(_Sensor):
    def full_name(self, components, stream):
        return '{}_streams_{}_{}'.format(components['sub'], stream, self.name)


# Sensor values to fetch via katportalclient, per input stream type
_SENSORS = {
    'cbf.antenna_channelised_voltage': [
        _CBFSensor('n_chans'),
        _CBFSensor('adc_sample_rate'),
        _CBFSensor('n_samples_between_spectra'),
        _SubSensor('bandwidth')
    ],
    'cbf.baseline_correlation_products': [
        _CBFSensor('int_time'),
        _CBFSensor('n_bls'),
        _CBFSensor('xeng_out_bits_per_sample'),
        _CBFSensor('n_chans_per_substream')
    ],
    'cbf.tied_array_channelised_voltage': [
        _CBFSensor('beng_out_bits_per_sample'),
        _CBFSensor('spectra_per_heap'),
        _CBFSensor('n_chans_per_substream')
    ]
}


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


def _input_channels(config, output):
    """Determine upper bound for `output_channels` of an output.

    Parameters
    ----------
    config : dict
        Product config
    output : dict
        Output entry within `config`, of a type that supports `output_channels`

    Returns
    -------
    int
        Maximum value for the upper bound in `output_channels`
    """
    if output['type'] in ['sdp.l0', 'sdp.vis', 'sdp.beamformer_engineering']:
        limits = []
        for src_name in output['src_streams']:
            src = config['inputs'][src_name]
            acv = config['inputs'][src['src_streams'][0]]
            limits.append(acv['n_chans'])
        return min(limits)
    elif output['type'] == 'sdp.spectral_image':
        flags_name = output['src_streams'][0]   # sdp.flags stream
        vis_name = config['outputs'][flags_name]['src_streams'][0]  # sdp.vis stream
        vis_config = config['outputs'][vis_name]
        if 'output_channels' in vis_config:
            c = vis_config['output_channels']
            n_channels = c[1] - c[0]
        else:
            n_channels = _input_channels(config, vis_config)
        return n_channels // vis_config['continuum_factor']
    else:
        raise NotImplementedError(
            'Unhandled stream type {}'.format(output['type']))   # pragma: nocover


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

    version = StrictVersion(config['version'])
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
        'sdp.flags': ['sdp.vis'],
        'sdp.continuum_image': ['sdp.flags'],
        'sdp.spectral_image': ['sdp.flags', 'sdp.continuum_image']
    }
    for name, stream in itertools.chain(config['inputs'].items(),
                                        config['outputs'].items()):
        src_streams = stream.get('src_streams', [])
        for i, src in enumerate(src_streams):
            if src in config['inputs']:
                src_config = config['inputs'][src]
            elif src in config['outputs']:
                src_config = config['outputs'][src]
            else:
                raise ValueError('Unknown source {} in {}'.format(src, name))
            valid_types = src_valid_types[stream['type']]
            # Special case: valid options depend on position
            if stream['type'] == 'sdp.spectral_image':
                valid_types = [valid_types[i]]
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
    # Sort the outputs so that we validate upstream outputs before the downstream
    # outputs that depend on them.
    OUTPUT_TYPE_ORDER = [
        'sdp.l0', 'sdp.vis', 'sdp.beamformer', 'sdp.beamformer_engineering',
        'sdp.cal', 'sdp.flags', 'sdp.continuum_image', 'sdp.spectral_image'
    ]
    output_items = sorted(
        config['outputs'].items(),
        key=lambda item: OUTPUT_TYPE_ORDER.index(item[1]['type']))
    for name, output in output_items:
        try:
            # Names of inputs and outputs must be disjoint
            if name in config['inputs']:
                raise ValueError('cannot be both an input and an output')

            # Channel ranges must be non-empty and not overflow
            if 'output_channels' in output:
                c = output['output_channels']
                limit = _input_channels(config, output)
                if not 0 <= c[0] < c[1] <= limit:
                    raise ValueError('Channel range {}:{} is invalid (valid range is {}:{})'
                                     .format(c[0], c[1], 0, limit))

            # Beamformer pols must have same channeliser
            if output['type'] in ['sdp.beamformer', 'sdp.beamformer_engineering']:
                common_acv = None
                for src_name in output['src_streams']:
                    src = config['inputs'][src_name]
                    acv_name = src['src_streams'][0]
                    if common_acv is not None and acv_name != common_acv:
                        raise ValueError('Source streams do not come from the same channeliser')
                    common_acv = acv_name

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
                if version < '2.2':
                    if calibration in has_flags:
                        raise ValueError('calibration ({}) already has a flags output'
                                         .format(calibration))
                    if output['src_streams'] != config['outputs'][calibration]['src_streams']:
                        raise ValueError('calibration ({}) has different src_streams'
                                         .format(calibration))
                else:
                    src_stream = output['src_streams'][0]
                    src_stream_config = copy.copy(config['outputs'][src_stream])
                    cal_config = config['outputs'][calibration]
                    cal_src_stream = cal_config['src_streams'][0]
                    cal_src_stream_config = copy.copy(config['outputs'][cal_src_stream])
                    src_cf = src_stream_config['continuum_factor']
                    cal_src_cf = cal_src_stream_config['continuum_factor']
                    if src_cf % cal_src_cf != 0:
                        raise ValueError('src_stream {} has bad continuum_factor relative to {}'
                                         .format(src_stream, cal_src_stream))
                    # Now delete attributes which aren't required to match to check that
                    # they match on the rest.
                    for attr in ['continuum_factor', 'archive']:
                        src_stream_config.pop(attr, None)
                        cal_src_stream_config.pop(attr, None)
                    if src_stream_config != cal_src_stream_config:
                        raise ValueError('src_stream {} does not match {}'
                                         .format(src_stream, cal_src_stream))
                has_flags.add(calibration)

        except ValueError as error:
            raise ValueError('{}: {}'.format(name, error)) from error


def _join_prefix(prefix, name):
    """Prepend `prefix` and a dot if `prefix` is non-empty."""
    return prefix + '.' + name if prefix else name


def _recursive_diff(a, b, prefix=''):
    """Provide human-readable explanation of the first difference found
    between two dicts, recursing into sub-dicts.

    The dicts must follow the JSON data model e.g. string keys, no cyclic
    references.
    """
    if not isinstance(a, dict) or not isinstance(b, dict):
        return '{} changed from {} to {}'.format(prefix, a, b)
    removed = sorted(set(a) - set(b))
    if removed:
        return '{} removed'.format(_join_prefix(prefix, removed[0]))
    added = sorted(set(b) - set(a))
    if added:
        return '{} added'.format(_join_prefix(prefix, added[0]))
    for key in sorted(a.keys()):
        if a[key] != b[key]:
            desc = str(key) if not prefix else prefix + '.' + str(key)
            return _recursive_diff(a[key], b[key], desc)
    return None


def validate_capture_block(product, capture_block):
    """Check that a capture block config is valid for a subarray product.

    Both parameters must have already been validated and normalised.

    Parameters
    ----------
    product : dict
        Subarray product config
    capture_block : dict
        Proposed capture block config

    Raises
    ------
    ValueError
        If `capture_block` is not valid.
    """
    product = copy.deepcopy(product)
    # We mutate (the copy of) product towards capture_block for each valid change
    # we find, then check that there are no more changes at the end.
    for name, output in list(product['outputs'].items()):
        if output['type'] in {'sdp.continuum_image', 'sdp.spectral_image'}:
            if name not in capture_block['outputs']:
                del product['outputs'][name]
            elif all(capture_block['outputs'].get(key) == output.get(key)
                     for key in ['type', 'src_streams', 'calibration']):
                product['outputs'][name] = copy.deepcopy(capture_block['outputs'][name])

    if product != capture_block:
        raise ValueError(_recursive_diff(product, capture_block))


async def update_from_sensors(config):
    """Compute an updated config using sensor values.

    The input must be validated but need not be normalised.

    This replaces attributes of CBF streams that correspond directly to
    sensors, by obtaining the values of the sensors. If there is no
    cam.http stream, no changes are made.
    """
    portal_url = None
    for stream in config['inputs'].values():
        if stream['type'] == 'cam.http':
            portal_url = stream['url']
            break
    if portal_url is None:
        return config

    config = copy.deepcopy(config)
    client = katportalclient.KATPortalClient(portal_url, None)
    components = {
        name: await client.sensor_subarray_lookup(name, None)
        for name in ['cbf', 'sub']
    }
    for stream_name, stream in config['inputs'].items():
        if stream.get('simulate', False):
            continue       # katportal won't know what values we're simulating for
        sensors = _SENSORS.get(stream['type'], [])
        for sensor in sensors:
            sample = None
            try:
                full_name = sensor.full_name(components, stream_name)
                sample = await client.sensor_value(full_name)
            except (katportalclient.SensorLookupError,
                    katportalclient.SensorNotFoundError,
                    katportalclient.InvalidResponseError) as exc:
                logger.warning('Could not get %s: %s', full_name, exc)
            else:
                if sample.status not in {'nominal', 'warn', 'error'}:
                    logger.warning('Sensor %s is in status %s', full_name, sample.status)
                    sample = None
            if sample is not None:
                if sensor.name not in stream:
                    logger.info('Setting %s %s to %s from sensor',
                                stream_name, sensor.name, sample.value)
                elif sample.value != stream[sensor.name]:
                    logger.warning('Changing %s %s from %s to %s from sensor',
                                   stream_name, sensor.name, stream[sensor.name], sample.value)
                stream[sensor.name] = sample.value
    return config


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
    def unique_name(base, current):
        if base not in current:
            return base
        seq = 0
        while True:
            proposed = '{}{}'.format(base, seq)
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
                config['outputs'][unique_name('cal', config['outputs'])] = cal

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
                config['outputs'][unique_name('sdp_l1_flags', config['outputs'])] = flags

    # Update to 2.4
    if config['version'] in ['2.0', '2.1', '2.2', '2.3']:
        # 2.4 is fully backwards-compatible to 2.0
        config['version'] = '2.4'

    # Fill in defaults
    for name, stream in config['inputs'].items():
        if stream['type'] in ['cbf.baseline_correlation_products',
                              'cbf.tied_array_channelised_voltage']:
            if stream.setdefault('simulate', False) is True:
                stream['simulate'] = {}
            if stream.get('simulate', False) is not False:
                stream['simulate'].setdefault('clock_ratio', 1.0)
                stream['simulate'].setdefault('sources', [])

    for name, output in config['outputs'].items():
        if output['type'] == 'sdp.vis':
            output.setdefault('excise', True)
        if output['type'] in ['sdp.vis', 'sdp.beamformer_engineering']:
            output.setdefault('output_channels', [0, _input_channels(config, output)])
        if output['type'] == 'sdp.cal':
            output.setdefault('parameters', {})
            output.setdefault('models', {})
        if output['type'] == 'sdp.continuum_image':
            output.setdefault('uvblavg_parameters', {})
            output.setdefault('mfimage_parameters', {})
        if output['type'] == 'sdp.spectral_image':
            output.setdefault('output_channels', [0, _input_channels(config, output)])

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
