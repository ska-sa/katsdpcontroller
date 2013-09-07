"""Core classes for the SDP Proxy.

"""

from katcp import DeviceServer, Sensor, Message
from katcp.kattypes import request, return_reply, Str, Int, Float

class SDPSubArray(object):
    """SDP Sub Array

    Represents an instance on an SDP subarray from the ingest perspective.
    """
    def __init__(self, subarray_id, n_antennas, n_channels, dump_rate, n_beams, sources):
        self.subarray_id = subarray_id
        self.n_antennas = n_antennas
        self.n_channels = n_channels
        self.dump_rate = dump_rate
        self.n_beams = n_beams
        self.sources = [s.split(":") for s in sources]
        if self.n_beams == 0:
           self.data_rate = (((n_antennas*(n_antennas+1))/2) * 4 * dump_rate * n_channels * 64) / 1e9
        else:
           self.data_rate = (n_beams * dump_rate * n_channels * 32) / 1e9

    def __repr__(self):
        return "Subarray %i: %i antennas, %i channels, %.2f dump_rate ==> %.2f Gibps" % (self.subarray_id, self.n_antennas, self.n_channels, self.dump_rate, self.data_rate)
		
class SDPProxyServer(DeviceServer):

    VERSION_INFO = ("sdpproxy", 0, 1)
    BUILD_INFO = ("sdpproxy", 0, 1, "rc2")

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

        self.subarrays = {}
         # dict of currently configured SDP subarrays

        super(SDPProxyServer, self).__init__(*args, **kwargs)

    def setup_sensors(self):
        """Add sensors for processes."""
        self.add_sensor(self._build_state_sensor)
        self.add_sensor(self._api_version_sensor)

    @request(Int(optional=True),Int(min=0,max=64,optional=True),Int(min=1,max=65535,optional=True),Float(optional=True),Int(min=0,max=16384,optional=True),Str(multiple=True,optional=True),include_msg=True)
    @return_reply(Str())
    def request_subarray_configure(self, req, req_msg, subarray_id, n_antennas, n_channels, dump_rate, n_beams, *sources):
        """Configure a SDP subarray instance.

        Inform Arguments
        ----------------
        subarray_id : int
            The ID to use for this subarray.
        n_antennas : int
            Number of antennas used in this subarray (based on CBF config)
            If n_antennas == 0, then this subarray is de-configured. Trailing arguments can be omitted.
        n_channels : int
            Number of channels used in this subarray (based on CBF config)
        dump_rate : float
            Dump rate of data product in Hz
        n_beams : int
            Number of beams in the data product (0 = Correlator output, 1+ = Beamformer)
        sources : list
            A space seperated list of source ip:port pairs used to receive data from CBF
        
        Returns
        -------
        success : {'ok', 'fail'}
            Whether the subarray was succesfully configured.
        """
        if not subarray_id:
            for (subarray_id,subarray) in self.subarrays.iteritems():
                req.inform(subarray_id,subarray)
            return ('ok',"%i" % len(self.subarrays));

        if not n_antennas >= 0:
            if subarray_id in self.subarrays:
                return ('ok',"%i is currently configured: %s" % (subarray_id,repr(self.subarrays[subarray_id])))
            else: return ('fail',"This subarray id has no current configuration.") 

        if n_antennas == 0:
            try:
                self.subarrays.pop(subarray_id)
                return ('ok',"Subarray has been deconfigured.")
            except KeyError:
                return ('fail',"Deconfiguration of subarray %i requested, but extant configuration found." % subarray_id)

        if subarray_id in self.subarrays:
            return ('ok',"Array already configured")

         # all good so far, lets check arguments for validity
        if not(n_antennas and n_channels >= 0 and dump_rate >= 0 and n_beams >= 0 and sources):
            return ('fail',"You must specify n_antennas, n_channels, dump_rate, n_beams and at least one source to configure a subarray")
        
        self.subarrays[subarray_id] = SDPSubArray(subarray_id, n_antennas, n_channels, dump_rate, n_beams, sources)

        return ('ok',"New array configured")

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



