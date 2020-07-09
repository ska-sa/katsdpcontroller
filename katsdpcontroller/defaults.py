"""Constants controlling tunable policies."""

#: GPU to target when not running develop mode
INGEST_GPU_NAME = 'GeForce GTX TITAN X'
#: Maximum number of custom signals requested by (correlator) timeplot
TIMEPLOT_MAX_CUSTOM_SIGNALS = 256
#: Target size of objects in the object store
WRITER_OBJECT_SIZE = 20e6    # 20 MB
#: Maximum channels per chunk for spectral imager
SPECTRAL_OBJECT_CHANNELS = 128
#: Minimum observation time for continuum imager (seconds)
CONTINUUM_MIN_TIME = 14 * 60.0     # 14 minutes
#: Minimum observation time for spectral imager (seconds)
SPECTRAL_MIN_TIME = 44 * 60.0      # 44 minutes
#: Size of cal buffer in seconds
CAL_BUFFER_TIME = 25 * 60.0        # 25 minutes (allows a single batch of 15 minutes)
#: Maximum number of scans to include in report
CAL_MAX_SCANS = 1000
#: Speed at which flags are transmitted, relative to real time
FLAGS_RATE_RATIO = 8.0
#: Alignment constraint for `int_time` in katcbfsim
KATCBFSIM_SPECTRA_PER_HEAP = 256
