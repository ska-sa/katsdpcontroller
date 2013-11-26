"""Core classes for the SDP Controller.

"""

import time
from katcp import DeviceServer, Sensor, Message
from katcp.kattypes import request, return_reply, Str, Int, Float

SA_STATES = {0:'unconfigured',1:'idle',2:'init_wait',3:'capturing',4:'capture_complete',5:'done'}

class SDPSubArray(object):
    """SDP Sub Array

    Represents an instance of an SDP data product. This includes ingest, an appropriate
    telescope model, and any required post-processing.

    """
    def __init__(self, data_product_id, antennas, n_channels, dump_rate, n_beams, sources):
        self.data_product_id = data_product_id
        self.antennas = antennas
        self.n_antennas = len(antennas.split(","))
        self.n_channels = n_channels
        self.dump_rate = dump_rate
        self.n_beams = n_beams
        self.sources = sources
        self._state = 0
        self.set_state(1)
        self.psb_id = 0
        if self.n_beams == 0:
           self.data_rate = (((self.n_antennas*(self.n_antennas+1))/2) * 4 * dump_rate * n_channels * 64) / 1e9
        else:
           self.data_rate = (n_beams * dump_rate * n_channels * 32) / 1e9

    def set_psb(self, psb_id):
        if self.psb_id > 0:
            return ('fail', 'An existing processing schedule block is already active. Please stop the data product before adding a new one.')
        if self._state < 2:
            return ('fail','The data product specified has not yet be inited. Please do this before init post processing.')
        self.psb_id = psb_id
        time.sleep(2) # simulation
        return ('ok','Post processing has been initialised')

    def get_psb(self, psb_id):
        if self.psb_id > 0:
            return ('ok','Post processing id %i is configured and active on this data product' % self.psb_id)
        return ('fail','No post processing block is active on this data product')

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
                return ('fail','Can only halt data_products that have been inited')
            self.force_done()
             # array is now idle again...
            return ('ok','Forced a capture done on data product')

        if state_id == 2:
            if self._state != 1:
                return ('fail','Data product is currently in state %s, not %s as expected. Cannot be inited.' % (self.state,SA_STATES[1]))
            time.sleep(2) # simulation

        self._set_state(state_id)
        return ('ok','State changed to %s' % self.state)

    def __repr__(self):
        return "Data product %s: %s antennas, %i channels, %.2f dump_rate ==> %.2f Gibps (State: %s, PSB ID: %i)" % (self.data_product_id, self.antennas, self.n_channels, self.dump_rate, self.data_rate, self.state, self.psb_id)

class SDPControllerServer(DeviceServer):

    VERSION_INFO = ("sdpcontroller", 0, 1)
    BUILD_INFO = ("sdpcontroller", 0, 1, "rc2")

    def __init__(self, *args, **kwargs):

         # setup sensors
        self._build_state_sensor = Sensor(Sensor.STRING, "build-state",
            "SDP Controller build state.", "")
        self._build_state_sensor.set_value(self.build_state())
        self._api_version_sensor = Sensor(Sensor.STRING, "api-version",
            "SDP Controller API version.", "")
        self._api_version_sensor.set_value(self.version())

        self.components = {}
         # dict of currently managed SDP components

        self.data_products = {}
         # dict of currently configured SDP data_products

        super(SDPControllerServer, self).__init__(*args, **kwargs)

    def setup_sensors(self):
        """Add sensors for processes."""
        self.add_sensor(self._build_state_sensor)
        self.add_sensor(self._api_version_sensor)

    @request(Str(optional=True),Str(optional=True),Int(min=1,max=65535,optional=True),Float(optional=True),Int(min=0,max=16384,optional=True),Str(optional=True),include_msg=True)
    @return_reply(Str())
    def request_data_product_configure(self, req, req_msg, data_product_id, antennas, n_channels, dump_rate, n_beams, sources):
        """Configure a SDP data product instance.

        Inform Arguments
        ----------------
        data_product_id : string
            The ID to use for this data product.
        antennas : string
            A space seperated list of antenna names to use in this data product.
            These will be matched to the CBF output and used to pull only the specific
            data.
            If antennas == "", then this data product is de-configured. Trailing arguments can be omitted.
        n_channels : int
            Number of channels used in this data product (based on CBF config)
        dump_rate : float
            Dump rate of data product in Hz
        n_beams : int
            Number of beams in the data product (0 = Correlator output, 1+ = Beamformer)
        sources : string
            A specification of the multicasts sources from which to receive data in the form <ip>[+<count>]:<port>
        
        Returns
        -------
        success : {'ok', 'fail'}
            Whether the data product was succesfully configured.
        """
        if not data_product_id:
            for (data_product_id,data_product) in self.data_products.iteritems():
                req.inform(data_product_id,data_product)
            return ('ok',"%i" % len(self.data_products));

        if antennas is None:
            if data_product_id in self.data_products:
                return ('ok',"%s is currently configured: %s" % (data_product_id,repr(self.data_products[data_product_id])))
            else: return ('fail',"This data product id has no current configuration.")

        if antennas == "":
            try:
                self.data_products.pop(data_product_id)
                return ('ok',"Data product has been deconfigured.")
            except KeyError:
                return ('fail',"Deconfiguration of data product %s requested, but extant configuration found." % data_product_id)

        if data_product_id in self.data_products:
            return ('ok',"Array already configured")

         # all good so far, lets check arguments for validity
        if not(antennas and n_channels >= 0 and dump_rate >= 0 and n_beams >= 0 and sources):
            return ('fail',"You must specify antennas, n_channels, dump_rate, n_beams and at least one source to configure a data product")

        self.data_products[data_product_id] = SDPSubArray(data_product_id, antennas, n_channels, dump_rate, n_beams, sources)

        return ('ok',"New array configured")

    @request(Str())
    @return_reply(Str())
    def request_capture_init(self, req, data_product_id):
        """Request capture of the specified data product to start.

        Note: This command is used to prepare the SDP for reception of data
        as specified by the data product provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product to initialise. This must have already been 
            configured via the data-product-configure command.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the system is ready to capture or not.
        """
        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        sa = self.data_products[data_product_id]

        rcode, rval = sa.set_state(2)
        if rcode == 'fail': return (rcode, rval)
         # attempt to set state to init
        return ('ok','SDP ready')

    @request(Str())
    @return_reply(Str())
    def request_capture_status(self, req, data_product_id):
        """Returns the status of the specified data product.

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product whose state we wish to return.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        return ('ok',self.data_products[data_product_id].state)

    @request(Str(),Int(optional=True))
    @return_reply(Str())
    def request_postproc_init(self, req, data_product_id, psb_id):
        """Returns the status of the specified data product.

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product that will provide data to the post processor
        psb_id : integer
            The id of the post processing schedule block to retrieve
            from the observations database that containts the configuration
            to apply to the post processor.

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        sa = self.data_products[data_product_id]

        if not psb_id >= 0:
            rcode, rval = sa.get_psb(psb_id)
            return (rcode, rval)

        rcode, rval = sa.set_psb(psb_id)
        return (rcode, rval)

    @request(Str())
    @return_reply(Str())
    def request_capture_done(self, req, data_product_id):
        """Halts the currently specified data product

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product whose state we wish to halt.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        rcode, rval = self.data_products[data_product_id].set_state(5)
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



