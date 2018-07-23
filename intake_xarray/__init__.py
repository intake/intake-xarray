from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .netcdf import NetCDFSource
from .raster import RasterIOSource
from .xzarr import ZarrSource
