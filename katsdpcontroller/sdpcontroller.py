"""Core classes for the SDP Controller.

"""

import time
import logging
import subprocess
import shlex
from katcp import DeviceServer, Sensor, Message, BlockingClient
from katcp.kattypes import request, return_reply, Str, Int, Float

SA_STATES = {0:'unconfigured',1:'idle',2:'init_wait',3:'capturing',4:'capture_complete',5:'done'}
TASK_STATES = {0:'init',1:'running',2:'killed'}

INGEST_BASE_PORT = 2040
 # base port to use for ingest processes

MAX_DATA_PRODUCTS = 255
 # the maximum possible number of simultaneously configured data products
 # pretty arbitrary, could be increased, but we need some limits

logger = logging.getLogger("katsdpcontroller.katsdpcontroller")

class SDPTask(object):
    """SDP Task wrapper.

    Represents an executing task within the scope of the SDP.

    Eventually this management will be fairly intelligent and will
    deploy and provision tasks automatically based on the available
    resources within the SDP.

    It is expected that SDPTask will be subclassed for specific types
    of execution.

    This is a very thin wrapper for now to support RTS.
    """
    def __init__(self, task_id, task_cmd, host):
        self.task_id = task_id
        self.task_cmd = task_cmd
        self._task_cmd_array = shlex.split(task_cmd)
        self.host = host
        self._task = None
        self.state = TASK_STATES[0]
        self.start_time = None

    def launch(self):
        try:
            self._task = subprocess.Popen(self._task_cmd_array)
            self.state = TASK_STATES[1]
            self.start_time = time.time()
            logger.info("Launched task ({}): {}".format(self.task_id, self.task_cmd))
        except OSError, err:
            retmsg = "Failed to launch SDP task. {0}".format(err)
            logger.error(retmsg)
            return ('fail',retmsg)
        return ('ok',"New task launched successfully")

    def halt(self):
        self._task.terminate()
        self.state = TASK_STATES[2]
        return ('ok',"Task terminated successfully.")

    def uptime(self):
        if self.start_time is None: return 0
        else: return time.time() - self.start_time

    def __repr__(self):
        return "SDP Task: status => {0}, uptime => {1:.2f}, cmd => {2}".format(self.state, self.uptime(), self._task_cmd_array[0])

class SDPDataProductBase(object):
    """SDP Data Product Base

    Represents an instance of an SDP data product. This includes ingest, an appropriate
    telescope model, and any required post-processing.

    In general each telescope data product is handled in a completely parallel fashion by the SDP.
    This class encapsulates these instances, handling control input and sensor feedback to CAM.

    ** This can be used directly as a stubbed simulator for use in standalone testing and validation. 
    It conforms to the functional interface, but does not launch tasks or generate data **
    """
    def __init__(self, data_product_id, antennas, n_channels, dump_rate, n_beams, sources, ingest_port):
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
        self.ingest_port = ingest_port
        self.ingest_host = 'localhost'
        self.ingest_process = None
        self.ingest_katcp = None
        if self.n_beams == 0:
           self.data_rate = (((self.n_antennas*(self.n_antennas+1))/2) * 4 * dump_rate * n_channels * 64) / 1e9
        else:
           self.data_rate = (n_beams * dump_rate * n_channels * 32) / 1e9
        logger.info("Created: {0}".format(self.__repr__()))

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
        if state_id == 5: state_id = 1
         # handle capture done in simulator
        self._state = state_id
        self.state = SA_STATES[self._state]
        return ('ok','')

    def deconfigure(self, force=False):
        if self._state == 1 or force:
            self._deconfigure()
            return ('ok', 'Data product has been deconfigured')
        else:
            return ('fail','Data product is not idle and thus cannot be deconfigured. Please issue capture_done first.')

    def _deconfigure(self):
        self._set_state(0)

    def set_state(self, state_id):
        # TODO: check that state change is allowed.
        if state_id == 5:
            if self._state < 2:
                return ('fail','Can only halt data_products that have been inited')

        if state_id == 2:
            if self._state != 1:
                return ('fail','Data product is currently in state %s, not %s as expected. Cannot be inited.' % (self.state,SA_STATES[1]))

        rcode, rval = self._set_state(state_id)
        if rcode == 'fail': return ('fail',rval)
        else:
            if rval == '': return ('ok','State changed to %s' % self.state)
        return ('ok', rval)

    def __repr__(self):
        return "Data product %s: %s antennas, %i channels, %.2f dump_rate ==> %.2f Gibps (State: %s, PSB ID: %i)" % (self.data_product_id, self.antennas, self.n_channels, self.dump_rate, self.data_rate, self.state, self.psb_id)

class SDPDataProduct(SDPDataProductBase):
    def __init__(self, *args, **kwargs):
        super(SDPDataProduct, self).__init__(*args, **kwargs)

    def connect(self):
        try:
            (data_host,data_port) = self.sources.split(":",2)
        except ValueError:
            retmsg = "Failed to parse CBF data source specification ({}), should be in the form <ip>[+<count>]:port".format(self.sources)
            logger.error(retmsg)
            return ('fail',retmsg)
        try:
            cmd = ["ingest.py","-p {0}".format(self.ingest_port),"--data-port={}".format(data_port),"--data-host={}".format(data_host)]
            self.ingest = subprocess.Popen(cmd)
            logger.info("Launching new ingest process with configuration: {}".format(cmd))
            self.ingest_katcp = BlockingClient(self.ingest_host, self.ingest_port)
            try:
                self.ingest_katcp.start(timeout=5)
                self.ingest_katcp.wait_connected(timeout=5)
            except RuntimeError:
                self.ingest.kill()
                self.ingest_katcp.stop()
                 # no need for these to lurk around
                retmsg = "Failed to connect to ingest process via katcp on host {0} and port {1}. Check to see if networking issues could be to blame.".format(self.ingest_host, self.ingest_port)
                logger.error(retmsg)
                return ('fail',retmsg)
        except OSError:
            retmsg = "Failed to launch ingest process for data product. Make sure that katspdingest is installed and ingest.py is in the path."
            logger.error(retmsg)
            return ('fail',retmsg)
        return ('ok','')

    def deconfigure(self, force=False):
        if self._state == 1 or force:
            if self._state != 1:
                logger.warning("Forcing capture_done on external request.")
                self._issue_req('capture-done')
            self._deconfigure()
            return ('ok', 'Data product has been deconfigured')
        else:
            return ('fail','Data product is not idle and thus cannot be deconfigured. Please issue capture_done first.')

    def _deconfigure(self):
        if self.ingest_katcp is not None:
            self.ingest_katcp.stop()
            self.ingest_katcp.join()
        if self.ingest is not None:
            self.ingest.terminate()

    def _issue_req(self, req):
        reply, informs = self.ingest_katcp.blocking_request(Message.request(req))
        if not reply.reply_ok():
            retmsg = "Failed to issue req to ingest. {0}".format(req, reply.arguments[-1])
            logger.warning(retmsg)
            return ('fail', retmsg)
        return ('ok', reply.arguments[-1])

    def _set_state(self, state_id):
        """The meat of the problem. Handles starting and stopping ingest processes and echo'ing requests."""
        rcode = 'ok'
        rval = ''
        if state_id == 2: rcode, rval = self._issue_req('capture-init')
        if state_id == 5:
            rcode, rval = self._issue_req('capture-done')

        if state_id == 5 or rcode == 'ok':
            if state_id == 5: state_id = 1
             # make sure that we dont get stuck if capture-done is failing...
            self._state = state_id
            self.state = SA_STATES[self._state]
        if rval == '': rval = "State changed to {0}".format(self.state)
        return (rcode, rval)

class SDPControllerServer(DeviceServer):

    VERSION_INFO = ("sdpcontroller", 0, 1)
    BUILD_INFO = ("sdpcontroller", 0, 1, "rc2")

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.INFO)
         # setup sensors
        self._build_state_sensor = Sensor(Sensor.STRING, "build-state",
            "SDP Controller build state.", "")
        self._build_state_sensor.set_value(self.build_state())
        self._api_version_sensor = Sensor(Sensor.STRING, "api-version",
            "SDP Controller API version.", "")
        self._api_version_sensor.set_value(self.version())
        self.simulate = args[2]
        if self.simulate: logger.warning("Note: Running in simulation mode...")
        self.components = {}
         # dict of currently managed SDP components

        self.data_products = {}
         # dict of currently configured SDP data_products
        self.ingest_ports = {}
        self.tasks = {}
         # dict of currently managed SDP tasks

        super(SDPControllerServer, self).__init__(*args, **kwargs)

    def setup_sensors(self):
        """Add sensors for processes."""
        self.add_sensor(self._build_state_sensor)
        self.add_sensor(self._api_version_sensor)

    @request(Str())
    @return_reply(Str())
    def request_task_terminate(self, req, task_id):
        """Terminate the specified SDP task.
        
        Inform Arguments
        ----------------
        task_id : string
            The ID of the task to terminate

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if not task_id in self.tasks: return ('fail',"Specified task ID ({0}) is unknown".format(task_id))
        task = self.tasks.pop(task_id)
        rcode, rval = task.halt()
        return (rcode, rval)


    @request(Str(optional=True),Str(optional=True),Str(optional=True))
    @return_reply(Str())
    def request_task_launch(self, req, task_id, task_cmd, host):
        """Launch a task within the SDP.
        This command allows tasks to be listed and launched within the SDP. Specification of a desired host
        is optional, as in general the master controller will decide on the most appropriate location on
        which to run the task.
        
        Inform Arguments
        ----------------
        task_id : string
            The unique ID used to identify this task.
            If empty then all managed tasks are listed.
        task_cmd : string
            The complete command to run including fully qualified executable and arguments
            If empty then the status of the specified id is shown
        host : string
            Force the controller to launch the task on the specified host

        Returns
        -------
        success : {'ok', 'fail'}
        host,port : If appropriate, the host/port pair to connect to the task via katcp is returned.
        """
        if not task_id:
            for (task_id, task) in self.tasks.iteritems():
                req.inform(task_id, task)
            return ('ok', "{0}".format(len(self.tasks)))

        if task_id in self.tasks:
            if not task_cmd: return ('ok',"{0}: {1}".format(task_id, self.tasks[task_id]))
            else: return ('fail',"A task with the specified ID is already running and cannot be reconfigured.")

        if task_id not in self.tasks and not task_cmd: return ('fail',"You must specify a command line to run for a new task")

        self.tasks[task_id] = SDPTask(task_id, task_cmd, host)
        rcode, rval = self.tasks[task_id].launch()
        if rcode == 'fail': self.tasks.pop(task_id)
         # launch failed, discard task
        return (rcode, rval)

    def deregister_product(self,data_product_id,force=False):
        """Deregister a data product.

        This first checks to make sure the product is in an appropriate state
        (ideally idle), and then shuts down the ingest and plotting
        processes associated with it.

        Forcing skips the check on state and is basically used in an emergency."""
        dp_handle = self.data_products[data_product_id]
        rcode, rval = dp_handle.deconfigure(force=force)
        if rcode == 'fail': return (rcode, rval)
             # cleanup signal displays (if any)
        disp_id = "{}_disp".format(data_product_id)
        if disp_id in self.tasks:
            disp_task = self.tasks.pop(disp_id)
            disp_task.halt()
        self.data_products.pop(data_product_id)
        self.ingest_ports.pop(data_product_id)
        return (rcode, rval)

    def handle_interrupt(self):
        """Try to shutdown as gracefully as possible when interrupted."""
        logger.warning("SDP Master Controller interrupted.")
        for data_product_id in self.data_products.keys():
            rcode, rval = self.deregister_product(data_product_id,force=True)
            logger.info("Deregistered data product {} ({},{})".format(data_product_id, rcode, rval))

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
            If ok, returns the port on which the ingest process for this product is running.
        """
        if not data_product_id:
            for (data_product_id,data_product) in self.data_products.iteritems():
                req.inform(data_product_id,data_product)
            return ('ok',"%i" % len(self.data_products))

        if antennas is None:
            if data_product_id in self.data_products:
                return ('ok',"%s is currently configured: %s" % (data_product_id,repr(self.data_products[data_product_id])))
            else: return ('fail',"This data product id has no current configuration.")

        if antennas == "":
            try:
                (rcode, rval) = self.deregister_product(data_product_id)
                return (rcode, rval)
            except KeyError:
                return ('fail',"Deconfiguration of data product %s requested, but no configuration found." % data_product_id)

        if data_product_id in self.data_products:
            dp = self.data_products[data_product_id]
            if dp.antennas == antennas and dp.n_channels == n_channels and dp.dump_rate == dump_rate and dp.n_beams == n_beams and dp.sources == sources:
                return ('ok',"Data product with this configuration already exists. Pass.")
            else:
                return ('fail',"A data product with this id ({}) already exists, but has a different configuration. Please deconfigure this product or choose a new product id to continue.".format(data_product_id))

         # all good so far, lets check arguments for validity
        if not(antennas and n_channels >= 0 and dump_rate >= 0 and n_beams >= 0 and sources):
            return ('fail',"You must specify antennas, n_channels, dump_rate, n_beams and at least one source to configure a data product")

         # determine a suitable port for ingest
        ingest_port = min([port+INGEST_BASE_PORT for port in range(MAX_DATA_PRODUCTS) if port+INGEST_BASE_PORT not in self.ingest_ports.values()])
        self.ingest_ports[data_product_id] = ingest_port
        disp_id = "{}_disp".format(data_product_id)

        if self.simulate: self.data_products[data_product_id] = SDPDataProductBase(data_product_id, antennas, n_channels, dump_rate, n_beams, sources, ingest_port)
        else:
            self.tasks[disp_id] = SDPTask(disp_id,"time_plot.py","127.0.0.1")
            self.tasks[disp_id].launch()
            self.data_products[data_product_id] = SDPDataProduct(data_product_id, antennas, n_channels, dump_rate, n_beams, sources, ingest_port)
            rcode, rval = self.data_products[data_product_id].connect()
            if rcode == 'fail':
                disp_task = self.tasks.pop(disp_id)
                if disp_task: disp_task.halt()
                self.data_products.pop(data_product_id)
                self.ingest_ports.pop(data_product_id)
                return (rcode, rval)
        return ('ok',str(ingest_port))

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

    @request(Str(optional=True))
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
        if not data_product_id:
            for (data_product_id,data_product) in self.data_products.iteritems():
                req.inform(data_product_id,data_product.state)
            return ('ok',"%i" % len(self.data_products))

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



