"""Core classes for the SDP Proxy.

"""

import time
from katcp import DeviceServer, Sensor, Message
from katcp.kattypes import request, return_reply, Str, Int, Float

SA_STATES = {0:'unconfigured',1:'idle',2:'init_wait',3:'capturing',4:'capture_complete',5:'done'}

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
        self._state = 0
        self.set_state(1)
        self.psb_id = 0
        if self.n_beams == 0:
           self.data_rate = (((n_antennas*(n_antennas+1))/2) * 4 * dump_rate * n_channels * 64) / 1e9
        else:
           self.data_rate = (n_beams * dump_rate * n_channels * 32) / 1e9

    def set_psb(self, psb_id):
        if self.psb_id > 0:
            return ('fail', 'An existing processing schedule block is already active. Please stop the subarray before adding a new one.')
        if self._state < 2:
            return ('fail','The subarray specified has not yet be inited. Please do this before init post processing.')
        self.psb_id = psb_id
        time.sleep(2) # simulation
        return ('ok','Post processing has been initialised')

    def get_psb(self, psb_id):
        if self.psb_id > 0:
            return ('ok','Post processing id %i is configured and active on this subarray' % self.psb_id)
        return ('fail','No post processing block is active on this subarray')

    def _set_state(self, state_id):
        self._state = state_id
        self.state = SA_STATES[self._state]

    def force_done(self):
        self._set_state(1)
        self.psb_id = 0
        time.sleep(2) # simulate

    def set_state(self, state_id):
        # TODO: check that state change is allowed.
        if state_id == 5:
            if self._state < 2:
                return ('fail','Can only halt subarrays that have been inited')
            self.force_done()
             # array is now idle again...
            return ('ok','Forced a capture done on subarray')

        if state_id == 2:
            if self._state != 1:
                return ('fail','Subarray is currently in state %s, not %s as expected. Cannot be inited.' % (self.state,SA_STATES[1]))
            time.sleep(2) # simulation

        self._set_state(state_id)
        return ('ok','State changed to %s' % self.state)

    def __repr__(self):
        return "Subarray %i: %i antennas, %i channels, %.2f dump_rate ==> %.2f Gibps (State: %s, PSB ID: %i)" % (self.subarray_id, self.n_antennas, self.n_channels, self.dump_rate, self.data_rate, self.state, self.psb_id)
		
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

    @request(Int())
    @return_reply(Str())
    def request_capture_init(self, req, subarray_id):
        """Request capture of the specified subarray to start.

        Note: This command is used to prepare the SDP for reception of data
        as specified by the subarray provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

        Inform Arguments
        ----------------
        subarray_id : integer
            The id of the subarray to use. This must have already been 
            configured via the subarray-configure command.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the system is ready to capture or not.
        """
        if subarray_id not in self.subarrays:
            return ('fail','No existing subarray configuration with this id found')
        sa = self.subarrays[subarray_id]

        rcode, rval = sa.set_state(2)
        if rcode == 'fail': return (rcode, rval)
         # attempt to set state to init
        return ('ok','SDP ready')

    @request(Int())
    @return_reply(Str())
    def request_capture_status(self, req, subarray_id):
        """Returns the status of the specified subarray.

        Inform Arguments
        ----------------
        subarray_id : integer
            The id of the subarray whose state we wish to return.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if subarray_id not in self.subarrays:
            return ('fail','No existing subarray configuration with this id found')
        return ('ok',self.subarrays[subarray_id].state)

    @request(Int(),Int(optional=True))
    @return_reply(Str())
    def request_postproc_init(self, req, subarray_id, psb_id):
        """Returns the status of the specified subarray.

        Inform Arguments
        ----------------
        subarray_id : integer
            The id of the subarray that will provide data to the post processor
        psb_id : integer
            The id of the post processing schedule block to retrieve
            from the observations database that containts the configuration
            to apply to the post processor.

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if subarray_id not in self.subarrays:
            return ('fail','No existing subarray configuration with this id found')
        sa = self.subarrays[subarray_id]

        if not psb_id >= 0:
            rcode, rval = sa.get_psb(psb_id)
            return (rcode, rval)

        rcode, rval = sa.set_psb(psb_id)
        return (rcode, rval)

    @request(Int())
    @return_reply(Str())
    def request_capture_done(self, req, subarray_id):
        """Halts the currently specified subarray

        Inform Arguments
        ----------------
        subarray_id : integer
            The id of the subarray whose state we wish to halt.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if subarray_id not in self.subarrays:
            return ('fail','No existing subarray configuration with this id found')
        rcode, rval = self.subarrays[subarray_id].set_state(5)
        return (rcode, rval)

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



