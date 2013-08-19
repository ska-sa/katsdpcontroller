"""Core classes for the SDP Proxy.

"""

from katcp import DeviceServer, Sensor, Message
from katcp.kattypes import request, return_reply, Str, Int, Float

class SDPProxyServer(DeviceServer):

    VERSION_INFO = ("sdpproxy", 0, 1)
    BUILD_INFO = ("sdpproxy", 0, 1, "rc1")

    def __init__(self, *args, **kwargs):

         # setup sensors
        self._build_state_sensor = Sensor(Sensor.STRING, "build-state",
            "SDP Proxy build state.", "")
        self._build_state_sensor.set_value(self.build_state())
        self._api_version_sensor = Sensor(Sensor.STRING, "api-version",
            "SDP Proxy API version.", "")
        self._api_version_sensor.set_value(self.version())

        self.components = {}
         # dict of currently managed SDP components

        super(SDPProxyServer, self).__init__(*args, **kwargs)

    def setup_sensors(self):
        """Add sensors for processes."""
        self.add_sensor(self._build_state_sensor)
        self.add_sensor(self._api_version_sensor)

    @request(include_msg=True)
    @return_reply(Int(min=0))
    def request_sdp_status(self, req, reqmsg):
        """Request status of SDP components.

        Inform Arguments
        ----------------
        process : str
            Name of a registered process.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether retrieving component status succeeded.
        informs : int
            Number of #sdp_status informs sent
        """
        for (component_name,component) in self.components:
            req.inform("%s:%s",component_name,component.status)
        return ("ok", len(self.components))



