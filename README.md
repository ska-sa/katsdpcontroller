katsdpcontroller
=================

MeerKAT Science Data Processor Controller - provides central control and monitoring services for the SDP.

The master controller is the first component to start, and last to stop, in the SDP software stack. It is responsible for bringup and shutdown of all subordinate processes and plays the central management role in the SDP.

All ingest processes are configured and controlled via the master controller.

