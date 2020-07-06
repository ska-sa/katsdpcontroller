"""Support for manipulating product config dictionaries."""

import collections.abc
import logging
import itertools
import copy
import math
from abc import ABC, abstractmethod
from distutils.version import StrictVersion
from typing import (
    Tuple, List, Dict, Mapping, AbstractSet, Sequence, Iterable,
    Union, ClassVar, Type, TypeVar, Optional, Any, cast, TYPE_CHECKING
)

import networkx
import yarl

import katpoint
from katsdptelstate.endpoint import endpoint_list_parser
import katportalclient

from . import schemas, defaults


logger = logging.getLogger(__name__)
_S = TypeVar('_S', bound='Stream')
_ValidTypes = Union[AbstractSet[str], Sequence[str]]
#: Number of bytes per complex visibility
BYTES_PER_VIS = 8
#: Number of bytes per per-visibility flag mask
BYTES_PER_FLAG = 1
#: Number of bytes per per-visibility weight
BYTES_PER_WEIGHT = 1
#: Number of bytes per vis-flags-weights combination
BYTES_PER_VFW = BYTES_PER_VIS + BYTES_PER_FLAG + BYTES_PER_WEIGHT


def _url_n_endpoints(url: Union[str, yarl.URL]) -> int:
    """Return the number of endpoints in a ``spead://`` URL.

    Parameters
    ----------
    url
        URL of the form spead://host[+N]:port

    Raises
    ------
    ValueError
        if `url` is not a valid URL, not a SPEAD url, or is missing a port.
    """
    url = yarl.URL(url)
    if url.scheme != 'spead':
        raise ValueError(f'non-spead URL {url}')
    if url.host is None:
        raise ValueError(f'URL {url} has no host')
    if url.port is None:
        raise ValueError(f'URL {url} has no port')
    return len(endpoint_list_parser(None)(url.host))


class SensorFailure(RuntimeError):
    """Failed to obtain a sensor value from katportal"""
    pass


class _Sensor(ABC):
    """A sensor that provides some data about an input stream.

    This is an abstract base class. Derived classes implement
    :meth:`full_name` to map the base name to the system-wide
    sensor name to query from katportal.
    """
    def __init__(self, name: str, type: Type) -> None:
        self.name = name
        self.type = type

    @abstractmethod
    def full_name(self, components: Mapping[str, str], stream: str, instrument: str) -> str:
        """Obtain system-wide name for the sensor.

        Parameters
        ----------
        components
            Maps logical component name (e.g. 'cbf') to actual component
            name (e.g. 'cbf_1').
        stream
            Name of the input stream (including instrument prefix)
        instrument
            Name of the instrument providing the stream, or ``None`` if
            the stream is not a CBF stream.
        """


class _CBFSensor(_Sensor):
    def full_name(self, components: Mapping[str, str], stream: str, instrument: str) -> str:
        return f'{components["cbf"]}_{stream}_{self.name}'


class _CBFInstrumentSensor(_Sensor):
    def full_name(self, components: Mapping[str, str], stream: str, instrument: str) -> str:
        return f'{components["cbf"]}_{instrument}_{self.name}'


class _SubStreamSensor(_Sensor):
    def full_name(self, components: Mapping[str, str], stream: str, instrument: str) -> str:
        return f'{components["sub"]}_streams_{stream}_{self.name}'


class _SubSensor(_Sensor):
    def full_name(self, components: Mapping[str, str], stream: str, instrument: str) -> str:
        return f'{components["sub"]}_{self.name}'


def _normalise_output_channels(
        n_chans: int,
        output_channels: Optional[Tuple[int, int]],
        alignment: int = 1) -> Tuple[int, int]:
    """Provide default for and validate `output_channels`, and align.

    If `output_channels` is ``None``, it will default to (0, `n_chans`). Otherwise,
    it will be widened so that both ends are multiples of `alignment`.

    Raises
    ------
    ValueError
        If the output range is empty or overflows (0, `n_chans`).
    ValueError
        If `n_chans` is not a multiple of `alignment`
    """
    if n_chans % alignment != 0:
        raise ValueError(f'n_chans ({n_chans}) '
                         f'is not a multiple of required alignment ({alignment})')
    c = output_channels    # Just for less typing
    if c is None:
        return (0, n_chans)
    elif c[0] >= c[1]:
        raise ValueError(f'output_channels is empty ({c[0]}:{c[1]})')
    elif c[0] < 0 or c[1] > n_chans:
        raise ValueError(f'output_channels ({c[0]}:{c[1]}) overflows valid range 0:{n_chans}')
    else:
        return (c[0] // alignment * alignment,
                (c[1] + alignment - 1) // alignment * alignment)
        return c


def data_rate(size: float, time: float, ratio: float = 1.05, overhead: float = 128) -> float:
    """Convert a heap size to a data rate in bits per second.

    Parameters
    ----------
    size
        Size in bytes
    time
        Time between heaps in seconds
    ratio
        Relative overhead
    overhead
        Absolute overhead, in bytes
    """
    return (size * ratio + overhead) * 8 / time


class ServiceOverride:
    """Debugging tool to modify how a service is run.

    Parameters
    ----------
    config
        Override the command-line arguments passed to the service through
        telescope state, using :func:`override`.
    taskinfo
        Override the task information given to Mesos to launch it, using
        :func:`override`.
    host
        Force the task to run on a specific host.
    """

    def __init__(self, *,
                 config: Mapping[str, Any] = {},
                 taskinfo: Mapping[str, Any] = {},
                 host: Optional[str] = None) -> None:
        self.config = dict(config)
        self.taskinfo = dict(taskinfo)
        self.host = host

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> 'ServiceOverride':
        return cls(
            config=config.get('config', {}),
            taskinfo=config.get('taskinfo', {}),
            host=config.get('host')
        )


class Options:
    def __init__(self, *, develop: bool = False,
                 wrapper: Optional[str] = None,
                 image_tag: Optional[str] = None,
                 service_overrides: Mapping[str, ServiceOverride] = {}) -> None:
        self.develop = develop
        self.wrapper = wrapper
        self.image_tag = image_tag
        self.service_overrides = dict(service_overrides)

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> 'Options':
        service_overrides = {
            name: ServiceOverride.from_config(value)
            for (name, value) in config.get('service_overrides', {}).items()
        }
        return cls(
            develop=config.get('develop', False),
            wrapper=config.get('wrapper'),
            image_tag=config.get('image_tag'),
            service_overrides=service_overrides
        )


class Simulation:
    def __init__(self, *, start_time: Optional[float] = None, clock_ratio: float = 1.0,
                 sources: Iterable[katpoint.Target] = ()) -> None:
        self.start_time = start_time
        self.clock_ratio = clock_ratio
        self.sources = list(sources)

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> 'Simulation':
        sources = []
        for i, desc in enumerate(config.get('sources', []), 1):
            try:
                source = katpoint.Target(desc)
            except Exception as exc:
                raise ValueError(f'Invalid source {i}: {exc}') from exc
            sources.append(source)
        return Simulation(
            start_time=config.get('start_time'),
            clock_ratio=config.get('clock_ratio', 1.0),
            sources=sources
        )


class Stream:
    """Base class for all streams."""

    stream_type: ClassVar[str]
    _class_sensors: ClassVar[Sequence[_Sensor]] = []
    # Types that are accepted for ``src_streams``. If it is a set, then any
    # element of ``src_streams`` may have any of the types in the set. If
    # it is a list, then the ith element of ``src_streams`` must have the
    # ith type in the list. The list may be longer than ``src_streams``
    # e.g. if there are optional elements.
    _valid_src_types: ClassVar[_ValidTypes] = set()

    def __init__(self, name: str, src_streams: Sequence['Stream']) -> None:
        self.name = name
        self.src_streams = list(src_streams)

    @classmethod
    @abstractmethod
    def from_config(cls: Type[_S],
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> _S: ...   # pragma: nocover


class CamHttpStream(Stream):
    """A stream for obtaining sensor values from katportal."""

    stream_type: ClassVar[str] = 'cam.http'

    def __init__(self, name: str, *, url: yarl.URL) -> None:
        super().__init__(name, [])
        self.url = url

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'CamHttpStream':
        assert not src_streams
        assert not sensors
        return cls(name, url=yarl.URL(config['url']))


class CbfStream:
    """Mix-in for real CBF streams.

    It exists mostly for isinstance tests.
    """

    url: yarl.URL
    instrument_dev_name: str


class AntennaChannelisedVoltageStreamBase(Stream):
    """Base for both simulated and real antenna-channelised-voltage streams."""

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 antennas: Iterable[str],
                 band: str,
                 n_chans: int,
                 bandwidth: float,
                 adc_sample_rate: float,
                 centre_frequency: float,
                 n_samples_between_spectra: int) -> None:
        super().__init__(name, src_streams)
        self.antennas = list(antennas)
        self.band = band
        self.n_chans = n_chans
        self.bandwidth = bandwidth
        self.centre_frequency = centre_frequency
        self.adc_sample_rate = adc_sample_rate
        self.n_samples_between_spectra = n_samples_between_spectra


class AntennaChannelisedVoltageStream(CbfStream, AntennaChannelisedVoltageStreamBase):
    """Real antenna-channelised-voltage stream."""

    stream_type: ClassVar[str] = 'cbf.antenna_channelised_voltage'
    _class_sensors: ClassVar[Sequence[_Sensor]] = [
        _CBFSensor('n_chans', int),
        _CBFInstrumentSensor('adc_sample_rate', float),
        _CBFSensor('n_samples_between_spectra', int),
        _SubStreamSensor('bandwidth', float),
        _SubStreamSensor('centre_frequency', float),
        _SubSensor('band', str)
    ]

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 url: yarl.URL,
                 antennas: Iterable[str],
                 band: str,
                 n_chans: int,
                 bandwidth: float,
                 adc_sample_rate: float,
                 centre_frequency: float,
                 n_samples_between_spectra: int,
                 instrument_dev_name: str) -> None:
        super().__init__(
            name, src_streams,
            antennas=antennas,
            band=band,
            n_chans=n_chans,
            bandwidth=bandwidth,
            adc_sample_rate=adc_sample_rate,
            centre_frequency=centre_frequency,
            n_samples_between_spectra=n_samples_between_spectra
        )
        self.url = url
        self.instrument_dev_name = instrument_dev_name

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'AntennaChannelisedVoltageStream':
        return cls(
            name, src_streams,
            url=yarl.URL(config['url']),
            antennas=config['antennas'],
            band=sensors['band'],
            n_chans=sensors['n_chans'],
            bandwidth=sensors['bandwidth'],
            adc_sample_rate=sensors['adc_sample_rate'],
            centre_frequency=sensors['centre_frequency'],
            n_samples_between_spectra=sensors['n_samples_between_spectra'],
            instrument_dev_name=config['instrument_dev_name']
        )


class SimAntennaChannelisedVoltageStream(AntennaChannelisedVoltageStreamBase):
    """Simulated antenna-channelised-voltage stream."""

    stream_type: ClassVar[str] = 'sim.cbf.antenna_channelised_voltage'

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 antennas: Iterable[katpoint.Antenna],
                 band: str,
                 n_chans: int,
                 bandwidth: float,
                 adc_sample_rate: float,
                 centre_frequency: float) -> None:
        self.antenna_objects = list(antennas)
        ratio = adc_sample_rate / (2 * bandwidth)
        if abs(ratio - round(ratio)) > 1e-6:
            raise ValueError('ADC Nyquist frequency is not a multiple of bandwidth')
        n_samples_between_spectra = round(n_chans * adc_sample_rate // bandwidth)
        super().__init__(
            name, src_streams,
            antennas=[antenna.name for antenna in self.antenna_objects],
            band=band,
            n_chans=n_chans,
            bandwidth=bandwidth,
            centre_frequency=centre_frequency,
            adc_sample_rate=adc_sample_rate,
            n_samples_between_spectra=n_samples_between_spectra
        )

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'SimAntennaChannelisedVoltageStream':
        antennas = []
        for desc in config['antennas']:
            try:
                antennas.append(katpoint.Antenna(desc))
            except Exception as exc:
                # katpoint can throw all kinds of exceptions
                raise ValueError(f'Invalid antenna description {desc!r}: {exc}') from exc
        return cls(
            name, src_streams,
            antennas=antennas,
            band=config['band'],
            n_chans=config['n_chans'],
            bandwidth=config['bandwidth'],
            adc_sample_rate=config['adc_sample_rate'],
            centre_frequency=config['centre_frequency']
        )


class CbfPerChannelStream(Stream):
    """Base for tied-array-channelised-voltage and baseline-correlation-products streams.

    It applies to both simulated and real streams.
    """

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 n_endpoints: int,
                 n_chans_per_substream: int,
                 bits_per_sample: int) -> None:
        super().__init__(name, src_streams)
        self.n_endpoints = n_endpoints
        self.n_chans_per_substream = n_chans_per_substream
        self.bits_per_sample = bits_per_sample

        if self.n_chans % self.n_endpoints != 0:
            raise ValueError(
                f'n_chans ({self.n_chans}) is not '
                f'a multiple of endpoints ({self.n_endpoints})')
        if self.n_chans_per_endpoint % self.n_chans_per_substream != 0:
            raise ValueError(
                f'channels per endpoint ({self.n_chans_per_endpoint}) '
                f'is not a multiple of channels per substream ({self.n_chans_per_substream})'
            )

    @property
    def antenna_channelised_voltage(self) -> 'AntennaChannelisedVoltageStreamBase':
        return cast(AntennaChannelisedVoltageStreamBase, self.src_streams[0])

    @property
    def antennas(self) -> Sequence[str]:
        """Antenna names."""
        return self.antenna_channelised_voltage.antennas

    @property
    def n_chans(self) -> int:
        """Number of channels."""
        return self.antenna_channelised_voltage.n_chans

    @property
    def n_chans_per_endpoint(self) -> int:
        return self.n_chans // self.n_endpoints

    @property
    def n_substreams(self) -> int:
        return self.n_chans // self.n_chans_per_substream

    @property
    def n_antennas(self) -> int:
        """Number of antennas."""
        return len(self.antennas)

    @property
    def bandwidth(self) -> float:
        """Output bandwidth, in Hz."""
        return self.antenna_channelised_voltage.bandwidth

    @property
    def centre_frequency(self) -> float:
        """Sky centre frequency, in Hz."""
        return self.antenna_channelised_voltage.centre_frequency

    @property
    def adc_sample_rate(self):
        """ADC sample rate, in Hz."""
        return self.antenna_channelised_voltage.adc_sample_rate

    @property
    def n_samples_between_spectra(self):
        """Number of ADC samples between spectra."""
        return self.antenna_channelised_voltage.n_samples_between_spectra

    @property
    @abstractmethod
    def size(self) -> int:
        """Size of a single frame in bytes."""

    @property
    @abstractmethod
    def int_time(self) -> float:
        """Time between heaps, in seconds."""

    def data_rate(self, ratio: float = 1.05, overhead: int = 128) -> float:
        """Network bandwidth in bits per second."""
        heap_size = self.size / self.n_substreams
        return data_rate(heap_size, self.int_time, ratio, overhead) * self.n_substreams


class BaselineCorrelationProductsStreamBase(CbfPerChannelStream):
    """Base for both simulated and real baseline-correlation-products streams."""

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 int_time: float,
                 n_endpoints: int,
                 n_chans_per_substream: int,
                 n_baselines: int,
                 bits_per_sample: int):
        super().__init__(
            name, src_streams,
            n_endpoints=n_endpoints,
            n_chans_per_substream=n_chans_per_substream,
            bits_per_sample=bits_per_sample
        )
        self._int_time = int_time
        self.n_baselines = n_baselines

    @property
    def int_time(self) -> float:
        return self._int_time

    @property
    def n_vis(self) -> int:
        return self.n_baselines * self.n_chans

    @property
    def size(self) -> int:
        """Size of frame in bytes"""
        return self.n_vis * 2 * self.bits_per_sample // 8


class BaselineCorrelationProductsStream(CbfStream, BaselineCorrelationProductsStreamBase):
    """Real baseline-correlation-products stream."""

    stream_type: ClassVar[str] = 'cbf.baseline-correlation-products'
    _class_sensors: ClassVar[Sequence[_Sensor]] = [
        _CBFSensor('int_time', float),
        _CBFSensor('n_bls', int),
        _CBFSensor('xeng_out_bits_per_sample', int),
        _CBFSensor('n_chans_per_substream', int)
    ]
    _valid_src_types: ClassVar[_ValidTypes] = {'cbf.antenna_channelised_voltage'}

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 url: yarl.URL,
                 int_time: float,
                 n_chans_per_substream: int,
                 n_baselines: int,
                 bits_per_sample: int,
                 instrument_dev_name: str) -> None:
        super().__init__(
            name, src_streams,
            int_time=int_time,
            n_endpoints=_url_n_endpoints(url),
            n_chans_per_substream=n_chans_per_substream,
            n_baselines=n_baselines,
            bits_per_sample=bits_per_sample
        )
        self.url = url
        self.instrument_dev_name = instrument_dev_name

    if TYPE_CHECKING:     # pragma: nocover
        # Refine the return type for mypy
        @property
        def antenna_channelised_voltage(self) -> AntennaChannelisedVoltageStream: ...

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'BaselineCorrelationProductsStream':
        return cls(
            name, src_streams,
            int_time=sensors['int_time'],
            url=yarl.URL(config['url']),
            n_chans_per_substream=sensors['n_chans_per_substream'],
            n_baselines=sensors['n_bls'],
            bits_per_sample=sensors['xeng_out_bits_per_sample'],
            instrument_dev_name=config['instrument_dev_name']
        )


class SimBaselineCorrelationProductsStream(BaselineCorrelationProductsStreamBase):
    """Simulated baseline-correlation-products stream."""

    stream_type: ClassVar[str] = 'sim.cbf.baseline-correlation-products'
    _valid_src_types: ClassVar[_ValidTypes] = {'sim.cbf.antenna_channelised_voltage'}

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 int_time: float,
                 n_endpoints: int,
                 n_chans_per_substream: Optional[int] = None) -> None:
        acv = cast(AntennaChannelisedVoltageStream, src_streams[0])
        if n_chans_per_substream is not None:
            ncps = n_chans_per_substream
        else:
            ncps = acv.n_chans // n_endpoints
        n_antennas = len(acv.antennas)
        # Round the int_time the same way katcbfsim does, so that we have
        # an accurate value.
        heap_time = acv.n_chans / acv.bandwidth * defaults.KATCBFSIM_SPECTRA_PER_HEAP
        acc_heaps = max(1, round(int_time / heap_time))
        int_time = acc_heaps * heap_time
        super().__init__(
            name, src_streams,
            int_time=int_time,
            n_endpoints=n_endpoints,
            n_chans_per_substream=ncps,
            n_baselines=n_antennas * (n_antennas + 1) * 2,
            bits_per_sample=32
        )

    if TYPE_CHECKING:     # pragma: nocover
        # Refine the return type for mypy
        @property
        def antenna_channelised_voltage(self) -> SimAntennaChannelisedVoltageStream: ...

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'SimBaselineCorrelationProductsStream':
        return cls(
            name, src_streams,
            int_time=config['int_time'],
            n_endpoints=config['n_endpoints'],
            n_chans_per_substream=config.get('n_chans_per_substream')
        )


class TiedArrayChannelisedVoltageStreamBase(CbfPerChannelStream):
    """Base for both simulated and real tied-array-channelised-voltage streams."""

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 n_endpoints: int,
                 n_chans_per_substream: int,
                 spectra_per_heap: int,
                 bits_per_sample: int) -> None:
        super().__init__(
            name, src_streams,
            n_endpoints=n_endpoints,
            n_chans_per_substream=n_chans_per_substream,
            bits_per_sample=bits_per_sample
        )
        self.spectra_per_heap = spectra_per_heap

    @property
    def size(self) -> int:
        """Size of frame in bytes."""
        return self.bits_per_sample * 2 * self.spectra_per_heap * self.n_chans // 8

    @property
    def int_time(self) -> float:
        """Interval between heaps, in seconds."""
        return self.spectra_per_heap * self.n_samples_between_spectra / self.adc_sample_rate


class TiedArrayChannelisedVoltageStream(CbfStream, TiedArrayChannelisedVoltageStreamBase):
    """Real tied-array-channelised-voltage stream."""

    stream_type: ClassVar[str] = 'cbf.tied_array_channelised_voltage'
    _class_sensors: ClassVar[Sequence[_Sensor]] = [
        _CBFSensor('beng_out_bits_per_sample', int),
        _CBFSensor('spectra_per_heap', int),
        _CBFSensor('n_chans_per_substream', int)
    ]
    _valid_src_types: ClassVar[_ValidTypes] = {'cbf.antenna_channelised_voltage'}

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 url: yarl.URL,
                 n_chans_per_substream: int,
                 spectra_per_heap: int,
                 bits_per_sample: int,
                 instrument_dev_name: str) -> None:
        super().__init__(
            name, src_streams,
            n_endpoints=_url_n_endpoints(url),
            n_chans_per_substream=n_chans_per_substream,
            spectra_per_heap=spectra_per_heap,
            bits_per_sample=bits_per_sample,
        )
        self.url = url
        self.instrument_dev_name = instrument_dev_name

    if TYPE_CHECKING:     # pragma: nocover
        # Refine the return type for mypy
        @property
        def antenna_channelised_voltage(self) -> AntennaChannelisedVoltageStream: ...

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'TiedArrayChannelisedVoltageStream':
        return cls(
            name, src_streams,
            url=yarl.URL(config['url']),
            n_chans_per_substream=sensors['n_chans_per_substream'],
            spectra_per_heap=sensors['spectra_per_heap'],
            bits_per_sample=sensors['beng_out_bits_per_sample'],
            instrument_dev_name=config['instrument_dev_name']
        )


class SimTiedArrayChannelisedVoltageStream(TiedArrayChannelisedVoltageStreamBase):
    """Simulated tied-array-channelised-voltage stream."""

    stream_type: ClassVar[str] = 'sim.cbf.tied_array_channelised_voltage'
    _valid_src_types: ClassVar[_ValidTypes] = {'sim.cbf.antenna_channelised_voltage'}

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 n_endpoints: int,
                 n_chans_per_substream: Optional[int] = None,
                 spectra_per_heap: int) -> None:
        acv = cast(AntennaChannelisedVoltageStream, src_streams[0])
        if n_chans_per_substream is not None:
            ncps = n_chans_per_substream
        else:
            ncps = acv.n_chans // n_endpoints
        super().__init__(
            name, src_streams,
            n_endpoints=n_endpoints,
            n_chans_per_substream=ncps,
            spectra_per_heap=spectra_per_heap,
            bits_per_sample=8
        )

    if TYPE_CHECKING:     # pragma: nocover
        # Refine the return type for mypy
        @property
        def antenna_channelised_voltage(self) -> SimAntennaChannelisedVoltageStream: ...

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'SimTiedArrayChannelisedVoltageStream':
        return cls(
            name, src_streams,
            n_endpoints=config['n_endpoints'],
            n_chans_per_substream=config.get('n_chans_per_substream'),
            spectra_per_heap=config.get('spectra_per_heap', defaults.KATCBFSIM_SPECTRA_PER_HEAP)
        )


class VisStream(Stream):
    """Instance of sdp.vis."""

    stream_type: ClassVar[str] = 'sdp.vis'
    _valid_src_types: ClassVar[_ValidTypes] = {
        'cbf.baseline_correlation_products',
        'sim.cbf.baseline_correlation_products'
    }

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 int_time: float,
                 output_channels: Optional[Tuple[int, int]] = None,
                 continuum_factor: int,
                 excise: bool,
                 archive: bool,
                 n_servers: int) -> None:
        super().__init__(name, src_streams)
        cbf_channels = self.baseline_correlation_products.n_chans
        cbf_int_time = self.baseline_correlation_products.int_time
        self.int_time = max(1, round(int_time / cbf_int_time)) * cbf_int_time
        c = _normalise_output_channels(cbf_channels, output_channels, n_servers * continuum_factor)
        self.output_channels = c
        self.continuum_factor = continuum_factor
        self.excise = excise
        self.archive = archive
        self.n_servers = n_servers

    @property
    def baseline_correlation_products(self) -> BaselineCorrelationProductsStreamBase:
        return cast(BaselineCorrelationProductsStreamBase, self.src_streams[0])

    @property
    def n_chans(self) -> int:
        rng = self.output_channels
        return (rng[1] - rng[0]) // self.continuum_factor

    @property
    def n_spectral_chans(self) -> int:
        """Number of CBF channels that are in the output, before continuum averaging."""
        return self.output_channels[1] - self.output_channels[0]

    @property
    def n_spectral_vis(self) -> int:
        """Number of visibilities per dump in the output, before continuum averaging."""
        return self.n_spectral_chans * self.n_baselines

    @property
    def antennas(self) -> Sequence[str]:
        return self.baseline_correlation_products.antennas

    @property
    def n_antennas(self) -> int:
        return self.baseline_correlation_products.n_antennas

    @property
    def n_pols(self) -> int:
        return 2          # TODO: get from config?

    @property
    def n_baselines(self) -> int:
        a = self.n_antennas
        return a * (a + 1) // 2 * self.n_pols**2

    @property
    def n_vis(self) -> int:
        return self.n_baselines * self.n_chans

    @property
    def size(self) -> int:
        """Size of each frame in bytes."""
        # complex64 for vis, uint8 for weights and flags, float32 for weights_channel
        return self.n_vis * BYTES_PER_VFW + self.n_chans * 4

    @property
    def flag_size(self):
        """Size of the flags in each frame, in bytes."""
        return self.n_vis * BYTES_PER_FLAG

    def data_rate(self, ratio: float = 1.05, overhead: int = 128) -> float:
        return data_rate(self.size, self.int_time, ratio, overhead)

    def flag_data_rate(self, ratio: float = 1.05, overhead: int = 128) -> float:
        return data_rate(self.flag_size, self.int_time, ratio, overhead)

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'VisStream':
        output_channels = config.get('output_channels')
        if output_channels is not None:
            output_channels = tuple(output_channels)
        return cls(
            name, src_streams,
            int_time=config['output_int_time'],
            output_channels=output_channels,
            continuum_factor=config['continuum_factor'],
            excise=config.get('excise', True),
            archive=config['archive'],
            n_servers=4 if not options.develop else 2
        )

    def compatible(self, other: 'VisStream') -> bool:
        """Determine whether the configurations are mostly the same.

        Specifically, they must be the same other than the values of
        ``name``, ``continuum_factor`` and ``archive``.
        """
        return all(getattr(self, name) == getattr(other, name)
                   for name in vars(self)
                   if name not in ['name', 'continuum_factor', 'archive'])


class BeamformerStreamBase(Stream):
    """Base for sdp.beamformer and sdp.beamformer_engineering streams."""

    _valid_src_types: ClassVar[_ValidTypes] = {
        'cbf.tied_array_channelised_voltage',
        'sim.cbf.tied_array_channelised_voltage'
    }

    def __init__(self, name: str, src_streams: Sequence[Stream]) -> None:
        super().__init__(name, src_streams)
        acv = self.antenna_channelised_voltage
        if not all(stream.src_streams[0] is acv for stream in src_streams):
            raise ValueError('Source streams do not come from the same channeliser')

    @property
    def antenna_channelised_voltage(self) -> AntennaChannelisedVoltageStreamBase:
        return cast(AntennaChannelisedVoltageStreamBase, self.src_streams[0].src_streams[0])

    @property
    def tied_array_channelised_voltage(self) -> Sequence[TiedArrayChannelisedVoltageStreamBase]:
        return [
            cast(TiedArrayChannelisedVoltageStreamBase, stream)
            for stream in self.src_streams
        ]

    @property
    def n_chans(self) -> int:
        return self.antenna_channelised_voltage.n_chans


class BeamformerStream(BeamformerStreamBase):
    """Instance of sdp.beamformer."""

    stream_type = 'sdp.beamformer'

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'BeamformerStream':
        return cls(name, src_streams)


class BeamformerEngineeringStream(BeamformerStreamBase):
    """Instance of sdp.beamformer_engineering."""

    stream_type = 'sdp.beamformer_engineering'

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 store: str,
                 output_channels: Optional[Tuple[int, int]] = None) -> None:
        super().__init__(name, src_streams)
        cbf_channels = self.antenna_channelised_voltage.n_chans
        c = _normalise_output_channels(cbf_channels, output_channels)
        for tacv in self.tied_array_channelised_voltage:
            c = _normalise_output_channels(cbf_channels, c, tacv.n_chans_per_endpoint)
        self.output_channels = c
        self.store = store

    @property
    def n_chans(self) -> int:
        return self.output_channels[1] - self.output_channels[0]

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'BeamformerEngineeringStream':
        output_channels = config.get('output_channels')
        if output_channels is not None:
            output_channels = tuple(output_channels)
        return cls(
            name, src_streams,
            store=config['store'],
            output_channels=output_channels
        )


class CalStream(Stream):
    """An instance of sdp.cal."""

    stream_type: ClassVar[str] = 'sdp.cal'
    _valid_src_types: ClassVar[_ValidTypes] = {'sdp.vis'}

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 parameters: Mapping[str, Any],
                 buffer_time: float,
                 max_scans: int) -> None:
        super().__init__(name, src_streams)
        if self.n_antennas < 4:
            raise ValueError(f'At least 4 antennas required but only {self.n_antennas} found')
        self.parameters = dict(parameters)
        self.buffer_time = buffer_time
        self.max_scans = max_scans

    @property
    def vis(self) -> VisStream:
        return cast(VisStream, self.src_streams[0])

    @property
    def n_antennas(self) -> int:
        return self.vis.n_antennas

    @property
    def slots(self) -> int:
        return int(math.ceil(self.buffer_time / self.vis.int_time))

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'CalStream':
        return cls(
            name, src_streams,
            parameters=config.get('parameters', {}),
            buffer_time=config.get('buffer_time', defaults.CAL_BUFFER_TIME),
            max_scans=config.get('max_scans', defaults.CAL_MAX_SCANS)
        )


class FlagsStream(Stream):
    """An instance of sdp.flags."""

    stream_type: ClassVar[str] = 'sdp.flags'
    _valid_src_types: ClassVar[_ValidTypes] = ['sdp.vis', 'sdp.cal']

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 rate_ratio: float,
                 archive: bool) -> None:
        super().__init__(name, src_streams)
        self.rate_ratio = rate_ratio
        self.archive = archive
        if not self.vis.compatible(self.cal.vis):
            raise ValueError(
                f'src_streams {self.vis.name}, {self.cal.vis.name} are incompatible')
        vis_cf = self.vis.continuum_factor
        cal_cf = self.cal.vis.continuum_factor
        if vis_cf % cal_cf != 0:
            raise ValueError(
                f'src_streams {self.vis.name}, {self.cal.vis.name} have '
                f'incompatible continuum factors {vis_cf}, {cal_cf}')

    @property
    def vis(self) -> VisStream:
        return cast(VisStream, self.src_streams[0])

    @property
    def cal(self) -> CalStream:
        return cast(CalStream, self.src_streams[1])

    @property
    def int_time(self) -> float:
        return self.vis.int_time

    @property
    def n_vis(self) -> int:
        return self.vis.n_vis

    @property
    def n_chans(self) -> int:
        return self.vis.n_chans

    @property
    def n_baselines(self) -> int:
        return self.vis.n_baselines

    @property
    def size(self) -> int:
        return self.vis.flag_size

    def data_rate(self, ratio: float = 1.05, overhead: int = 128) -> float:
        return self.vis.flag_data_rate(ratio, overhead) * self.rate_ratio

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'FlagsStream':
        return cls(
            name, src_streams,
            rate_ratio=config.get('rate_ratio', defaults.FLAGS_RATE_RATIO),
            archive=config['archive']
        )


class ImageStream(Stream):
    """A base class for spectral and continuum image streams."""

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 min_time: float) -> None:
        super().__init__(name, src_streams)
        self.min_time = min_time

    @property
    def flags(self) -> FlagsStream:
        return cast(FlagsStream, self.src_streams[0])

    @property
    def vis(self) -> VisStream:
        return self.flags.vis

    @property
    def cal(self) -> CalStream:
        return self.flags.cal


class ContinuumImageStream(ImageStream):
    """An instance of sdp.continuum_image."""

    stream_type: ClassVar[str] = 'continuum_image'
    _valid_src_types: ClassVar[_ValidTypes] = {'sdp.flags'}

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 uvblavg_parameters: Mapping[str, Any],
                 mfimage_parameters: Mapping[str, Any],
                 max_realtime: Optional[float],
                 min_time: float) -> None:
        super().__init__(name, src_streams, min_time=min_time)
        self.uvblavg_parameters = dict(uvblavg_parameters)
        self.mfimage_parameters = dict(mfimage_parameters)
        self.max_realtime = max_realtime

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'ContinuumImageStream':
        return cls(
            name, src_streams,
            uvblavg_parameters=config.get('uvblavg_parameters', {}),
            mfimage_parameters=config.get('mfimage_parameters', {}),
            max_realtime=config.get('max_realtime'),
            min_time=config.get('min_time', defaults.CONTINUUM_MIN_TIME)
        )


class SpectralImageStream(ImageStream):
    """An instance of sdp.spectral_image."""

    stream_type: ClassVar[str] = 'spectral_image'
    _valid_src_types: ClassVar[_ValidTypes] = ['sdp.flags', 'sdp.continuum_image']

    def __init__(self, name: str, src_streams: Sequence[Stream], *,
                 output_channels: Optional[Tuple[int, int]] = None,
                 parameters: Mapping[str, Any],
                 min_time: float) -> None:
        super().__init__(name, src_streams, min_time=min_time)
        self.parameters = dict(parameters)
        vis_channels = self.vis.n_chans
        self.output_channels = _normalise_output_channels(vis_channels, output_channels)

    @property
    def n_chans(self) -> int:
        return self.output_channels[1] - self.output_channels[0]

    @property
    def flags(self) -> FlagsStream:
        return cast(FlagsStream, self.src_streams[0])

    @property
    def continuum(self) -> Optional[ContinuumImageStream]:
        try:
            return cast(ContinuumImageStream, self.src_streams[1])
        except IndexError:
            return None

    @classmethod
    def from_config(cls,
                    options: Options,
                    name: str,
                    config: Mapping[str, Any],
                    src_streams: Sequence['Stream'],
                    sensors: Mapping[str, Any]) -> 'SpectralImageStream':
        output_channels = config.get('output_channels')
        if output_channels is not None:
            output_channels = tuple(output_channels)
        return cls(
            name, src_streams,
            output_channels=output_channels,
            parameters=config.get('parameters', {}),
            min_time=config.get('min_time', defaults.SPECTRAL_MIN_TIME)
        )


STREAM_CLASSES: Mapping[str, Type[Stream]] = {
    'cbf.antenna_channelised_voltage': AntennaChannelisedVoltageStream,
    'cbf.tied_array_channelised_voltage': TiedArrayChannelisedVoltageStream,
    'cbf.baseline_correlation_products': BaselineCorrelationProductsStream,
    'sim.cbf.antenna_channelised_voltage': SimAntennaChannelisedVoltageStream,
    'sim.cbf.tied_array_channelised_voltage': SimTiedArrayChannelisedVoltageStream,
    'sim.cbf.baseline_correlation_products': SimBaselineCorrelationProductsStream,
    'cam.http': CamHttpStream,
    'sdp.vis': VisStream,
    'sdp.beamformer': BeamformerStream,
    'sdp.beamformer_engineering': BeamformerEngineeringStream,
    'sdp.cal': CalStream,
    'sdp.flags': FlagsStream,
    'sdp.continuum_image': ContinuumImageStream,
    'sdp.spectral_image': SpectralImageStream
}


class Configuration:
    def __init__(self, *,
                 options: Options,
                 simulation: Simulation,
                 streams: Iterable[Stream]) -> None:
        self.options = options
        self.simulation = simulation
        self.streams = list(streams)
        self._by_class: Dict[Type[Stream], List[Stream]] = {}
        for stream in streams:
            self._by_class.setdefault(type(stream), []).append(stream)

    def by_class(self, stream_cls: Type[_S]) -> Sequence[_S]:
        return self._by_class.get(stream_cls, [])    # type: ignore

    @classmethod
    async def from_config(cls, config: Mapping[str, Any]) -> 'Configuration':
        _validate(config)
        config = _upgrade(config)
        options = Options.from_config(config.get('config', {}))
        simulation = Simulation.from_config(config.get('simulation', {}))
        # First get the cam.http stream, so that sensors can be extracted
        cam_http: Optional[CamHttpStream] = None
        stream_configs = {**config.get('inputs', {}), **config.get('outputs', {})}
        for name, stream_config in stream_configs.items():
            if stream_config['type'] == 'cam.http':
                cam_http = CamHttpStream.from_config(options, name, stream_config, [], {})
                break

        # Extract sensor values. This is pulled into a separate block rather
        # than done as we construct each stream so that one day it can be
        # optimised to make a single sensor_values call for all sensors.
        sensors: Dict[str, Dict[str, Any]] = {name: {} for name in stream_configs}
        if cam_http:
            client = katportalclient.KATPortalClient(str(cam_http.url), None)
            components = {}
            for name in ['cbf', 'sub']:
                try:
                    components[name] = await client.sensor_subarray_lookup(name, None)
                except Exception as exc:
                    # There are too many possible exceptions from katportalclient to
                    # try to list them all explicitly.
                    raise SensorFailure(f'Could not get component name for {name}: {exc}') from exc
            for name, stream_config in stream_configs.items():
                stream_cls = STREAM_CLASSES[stream_config['type']]
                for sensor in stream_cls._class_sensors:
                    instrument = stream_config['instrument_dev_name']
                    full_name = sensor.full_name(components, name, instrument)
                    try:
                        sample = await client.sensor_value(full_name)
                    except Exception as exc:
                        raise SensorFailure(f'Could not get value for {full_name}: {exc}') from exc
                    if sample.status not in {'nominal', 'warn', 'error'}:
                        raise SensorFailure(
                            f'Sensor {full_name} has expected status {sample.status}'
                        )
                    if not isinstance(sample.value, sensor.type):
                        actual_type = type(sample.value)
                        raise SensorFailure(
                            f'Sensor {full_name} has type {actual_type} instead of {sensor.type}')
                    sensors[name][sensor.name] = sample.value

        # Build a dependency graph so that we build the streams in order.
        # Note that _validate checks the types of source streams, which
        # rules out cyclic dependencies.
        g = networkx.MultiDiGraph()
        g.add_nodes_from(stream_configs)
        for name, stream_config in stream_configs.items():
            for dep in stream_config.get('src_streams', []):
                g.add_edge(dep, name)    # Need to build dep before name

        streams: Dict[str, Stream] = {}
        # Construct the streams. Note that this will make another copy of
        # the cam.http stream, but that is harmless.
        for name in networkx.topological_sort(g):
            stream_config = stream_configs[name]
            stream_cls = STREAM_CLASSES[stream_config['type']]
            src_streams = [streams[d] for d in stream_config.get('src_streams', [])]
            try:
                streams[name] = stream_cls.from_config(
                    options, name, stream_config, src_streams, sensors[name])
            except ValueError as exc:
                raise ValueError(f'Configuration error for stream {name}: {exc}') from exc
        return cls(options=options, simulation=simulation, streams=streams.values())


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


def _validate(config):
    """Do initial validation on a config dict.

    This ensures that it adheres to the schema and satisfies some minimal
    constraints that are needed for :func:`_upgrade` and
    :meth:`Configuration.from_config` to function without breaking.

    .. note::

       Some pre-3.0 configurations that would have been rejected in earlier
       versions will now pass validation. This occurs where certain information
       provided in those configurations is discarded during the migration to
       3.x.

    Raises
    ------
    jsonschema.ValidationError
        if the config doesn't conform to the schema
    ValueError
        if semantic constraints are violated
    """
    schemas.PRODUCT_CONFIG.validate(config)
    version = StrictVersion(config['version'])
    inputs = config.get('inputs', {})
    outputs = config.get('outputs', {})
    for name, stream in itertools.chain(inputs.items(), outputs.items()):
        src_streams = stream.get('src_streams', [])
        valid_types = STREAM_CLASSES[stream['type']]._valid_src_types
        for i, src in enumerate(src_streams):
            if src in inputs:
                src_config = inputs[src]
            elif src in outputs:
                src_config = outputs[src]
            else:
                raise ValueError(f'Unknown source {src} in {name}')
            if isinstance(valid_types, collections.abc.Set):
                valid_type = src_config['type'] in valid_types
            else:
                valid_type = src_config['type'] == valid_types[i]
            if not valid_type:
                raise ValueError(f'Source {src} has wrong type for {name}')

    have_cam_http = False
    for name, stream in inputs.items():
        # It's not possible to convert 2.x simulations to 3.0 because we don't
        # know the band.
        if stream.get('simulate', False) is not False:
            raise ValueError(f'Version {config["version"]} with simulation is not supported')
        if stream['type'] == 'cam.http':
            if have_cam_http:
                raise ValueError('Cannot have more than one cam.http stream')
            have_cam_http = True

    if inputs and not have_cam_http:
        raise ValueError('A cam.http stream is required if there are any inputs')

    for name, output in outputs.items():
        try:
            # Names of inputs and outputs must be disjoint
            if name in inputs:
                raise ValueError('cannot be both an input and an output')

            if output['type'] == 'sdp.cal':
                if output.get('models', {}):
                    raise ValueError('sdp.cal output type no longer supports models')

            if output['type'] == 'sdp.flags':
                if version < '3.0':
                    calibration = output['calibration'][0]
                    if calibration not in outputs:
                        raise ValueError('calibration ({}) does not exist'.format(calibration))
                    elif outputs[calibration]['type'] != 'sdp.cal':
                        raise ValueError('calibration ({}) has wrong type {}'
                                         .format(calibration,
                                                 outputs[calibration]['type']))

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


def validate_capture_block(product: Dict[str, Any], capture_block: Dict[str, Any]):
    """Check that a capture block config is valid for a subarray product.

    Both parameters must have already been validated, in that
    :meth:`Configuration.from_config` must have been successful.

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


def _upgrade(config):
    """Convert a config dictionary to the newest version and return it.

    It is assumed to already have passed :func:`_validate`.
    """
    config = copy.deepcopy(config)
    config.setdefault('inputs', {})
    config.setdefault('outputs', {})
    # Update to 3.0
    if config['version'] < StrictVersion('3.0'):
        # Transfer only recognised stream types and parameters from inputs
        orig_inputs = config['inputs']
        config['inputs'] = {}
        for name, stream in orig_inputs.items():
            copy_keys = {'type', 'url', 'src_streams'}
            if stream['type'] == 'cbf.antenna_channelised_voltage':
                copy_keys |= {'antennas', 'instrument_dev_name'}
            elif stream['type'] == 'cbf.baseline_correlation_products':
                copy_keys |= {'instrument_dev_name'}
            elif stream['type'] == 'cbf.tied_array_channelised_voltage':
                copy_keys |= {'instrument_dev_name'}
            elif stream['type'] == 'cam.http':
                pass
            else:
                assert stream['type'] not in STREAM_CLASSES
                # The next line is actually covered, but due to
                # https://bugs.python.org/issue2506 is not detected.
                continue     # pragma: nocover
            new_stream = {}
            for key in copy_keys:
                if key in stream:
                    new_stream[key] = stream[key]
            config['inputs'][name] = new_stream

        # Remove calibration and imaging if less than 4 antennas
        to_remove = []
        req_ants = {'sdp.cal', 'sdp.flags', 'sdp.continuum_image', 'sdp.spectral_image'}
        for name, output in config['outputs'].items():
            if output['type'] in req_ants:
                src = name
                # Follow the chain to find the antenna-channelised-voltage stream
                while src in config['outputs']:
                    src = config['outputs'][src]['src_streams'][0]
                while 'antennas' not in config['inputs'][src]:
                    src = config['inputs'][src]['src_streams'][0]
                n_antennas = len(config['inputs'][src]['antennas'])
                if n_antennas < 4:
                    to_remove.append(name)
        for name in to_remove:
            del config['outputs'][name]

        # Convert sdp.flags.calibration to src_stream
        for name, output in config['outputs'].items():
            if output['type'] == 'sdp.flags':
                output['src_streams'].append(output['calibration'][0])
                del output['calibration']

        config['version'] = '3.0'

    _validate(config)     # Should never fail if the input was valid
    return config
