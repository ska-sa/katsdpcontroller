"""Mock implementation of :class:`katportalclient.KATPortalClient`."""

import re

import tornado.gen
import katportalclient


def _make_sample(value):
    if isinstance(value, katportalclient.SensorSample):
        return value
    else:
        return katportalclient.SensorSample(1234567890.0, value, 'nominal')


class KATPortalClient:
    """Mock implementation of :class:`katportalclient.KATPortalClient`.

    It only implements a handful of methods necessary for the unit tests.
    To pick up any conflicts between tornado and asyncio, it uses tornado
    coroutines.

    Parameters
    ----------
    components : Mapping[str, str]
        Maps abstract component names to proxy names
    sensors : Mapping[str, Any]
        Maps full sensor name to sensor value. If the value is a
        :class:`katportalclient.SensorSample` it is used as is, otherwise a
        sample is created with an arbitrary timestamp and nominal status.
    """

    def __init__(self, components, sensors):
        self.components = components
        self.sensors = {key: _make_sample(value) for (key, value) in sensors.items()}

    @tornado.gen.coroutine
    def sensor_subarray_lookup(self, component, sensor, return_katcp_name=False):
        assert not return_katcp_name    # Not implemented and not needed
        assert sensor is None           # Alternative not implemented
        try:
            return self.components[component]
        except KeyError:
            raise katportalclient.SensorLookupError('Not such component {}'
                                                    .format(component)) from None

    @tornado.gen.coroutine
    def sensor_values(self, filters, include_value_ts=False):
        assert not include_value_ts     # Not implemented and not needed
        if isinstance(filters, str):
            filters = [filters]
        results = {}
        for filt in filters:
            pattern = re.compile(filt)
            filt_results = {}
            for sensor_name, sample in self.sensors.items():
                if pattern.search(sensor_name):
                    filt_results[sensor_name] = sample
            if not filt_results:
                raise katportalclient.SensorNotFoundError(f'No values for filter {filt} found')
            results.update(filt_results)
        return results
