################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Constants controlling tunable policies."""

#: Unlike "localhost", guaranteed to be IPv4, which is more compatible with Docker
LOCALHOST = "127.0.0.1"
#: GPU to target when not running develop mode
INGEST_GPU_NAME = "NVIDIA A30"
#: Maximum number of custom signals requested by (correlator) timeplot
TIMEPLOT_MAX_CUSTOM_SIGNALS = 256
#: Target size of objects in the object store
WRITER_OBJECT_SIZE = 20e6  # 20 MB
#: Maximum channels per chunk for spectral imager
SPECTRAL_OBJECT_CHANNELS = 128
#: Minimum observation time for continuum imager (seconds)
#: This, and SPECTRAL_MIN_TIME, are set to one minute
#: less than some typical observations lengths to avoid
#: off by one dump triggering issues.
CONTINUUM_MIN_TIME = 14 * 60.0  # 14 minutes
#: Minimum observation time for spectral imager (seconds)
SPECTRAL_MIN_TIME = 44 * 60.0  # 44 minutes
#: Size of cal buffer in seconds
CAL_BUFFER_TIME = 25 * 60.0  # 25 minutes (allows a single batch of 15 minutes)
#: Maximum number of scans to include in report
CAL_MAX_SCANS = 1000
#: Speed at which flags are transmitted, relative to real time
FLAGS_RATE_RATIO = 8.0
#: Alignment constraint for `int_time` in katcbfsim
KATCBFSIM_SPECTRA_PER_HEAP = 256
#: Alignment constraint for `int_time` in xgpu
GPUCBF_JONES_PER_BATCH = 2**20
#: Maximum input data rate for an xbgpu instance (bytes per second).
#: This is sufficient for UHF with 80 antennas to be handled by 32 engines.
XBGPU_MAX_SRC_DATA_RATE = 5.45e9
#: Number of polyphase filter-bank taps for gpucbf F-engines
PFB_TAPS = 16
#: Multiply by decimation factor to get digitiser down-conversion taps
#: for gpucbf F-engines.
DDC_TAPS_RATIO = 12
#: Default payload size for gpucbf data products. Bigger is better to
#: minimise the number of packets/second to process.
GPUCBF_PACKET_PAYLOAD_BYTES = 8192
#: Minimum update period (in seconds) for katcp sensors which aggregate many
#: underlying sensors and hence may be updated much more often than any one
#: of the underlying sensors.
FAST_SENSOR_UPDATE_PERIOD = 1.0
#: Autotune fallback behaviour: "nearest" or "exact"
KATSDPSIGPROC_TUNE_MATCH = "nearest"
#: Time (in seconds) to sleep before exiting
SHUTDOWN_DELAY = 10.0
#: Time to wait for rx.device-status sensors to become nominal
RX_DEVICE_STATUS_TIMEOUT = 30.0
#: Maximum number of bytes in a connection backlog before disconnecting a
#: katcp client. This is increased compared to the aiokatcp default because
#: some sensors are large strings and a batch of sensor updates could
#: temporarily cause a large backlog.
CONNECTION_MAX_BACKLOG = 256 * 1024 * 1024
