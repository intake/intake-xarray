from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import intake_xarray.base
import intake
from .netcdf import NetCDFSource
from .opendap import OpenDapSource
from .raster import RasterIOSource
#from .xzarr import ZarrSource
from .image import ImageSource
