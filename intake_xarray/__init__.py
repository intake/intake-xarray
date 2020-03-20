from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import intake  # Import this first to avoid circular imports during discovery.
from intake.container import register_container
from .netcdf import NetCDFSource
from .opendap import OpenDapSource
from .raster import RasterIOSource
from .xzarr import ZarrSource
from .xarray_container import RemoteXarray
from .image import ImageSource


intake.register_driver('remote-xarray', RemoteXarray)
register_container('xarray', RemoteXarray)
