"""Mock implementation of :class:`katportalclient.KATPortalClient`."""

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
        self._components = components
        self._sensors = {key: _make_sample(value) for (key, value) in sensors.items()}

    @tornado.gen.coroutine
    def sensor_subarray_lookup(self, component, sensor, return_katcp_name=False):
        assert not return_katcp_name    # Not implemented and not needed
        assert sensor is None           # Alternative not implemented
        try:
            return self._components[component]
        except KeyError:
            raise katportalclient.SensorLookupError('Not such component {}'
                                                    .format(component)) from None

    @tornado.gen.coroutine
    def sensor_value(self, sensor_name, include_value_ts=False):
        assert not include_value_ts     # Not implemented and not needed
        try:
            return self._sensors[sensor_name]
        except KeyError:
            raise katportalclient.SensorNotFoundError('Value for sensor {} not found'
                                                      .format(sensor_name)) from None
