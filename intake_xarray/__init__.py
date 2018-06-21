from intake.source import base
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class NetCDFPlugin(base.Plugin):
    """Plugin for netcdf->xarray reader"""

    def __init__(self):
        super(NetCDFPlugin, self).__init__(
            name='netcdf',
            version=__version__,
            container='xarray',
            partition_access=True)

    def open(self, urlpath, chunks, **kwargs):
        """
        Create NetCDFSource instance

        Open any file types available to xarray.

        If `pynio`_ is installed, additional file-types such as GRIB(2) are
        supported. Use the keyword ``engine='pynio'``.

        .. pynio: https://github.com/NCAR/pynio

        If passing multiple files, kwargs should include ``concat_dim=``
        keyword, the dimension name to give the file index.

        Parameters
        ----------
        urlpath: str
            Path to source file(s).
        chunks: int or dict
            Chunks is used to load the new dataset into dask
            arrays. Use ``chunks={}`` to load without chunking.
        kwargs:
            Further parameters are passed to xr.open_dataset or
            xr.open_mfdataset .
        """
        from intake_xarray.netcdf import NetCDFSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return NetCDFSource(
            urlpath=urlpath,
            chunks=chunks,
            xarray_kwargs=source_kwargs,
            metadata=base_kwargs['metadata'])


class RasterIOPlugin(base.Plugin):
    """Plugin for xarray reader via rasterIO"""

    def __init__(self):
        super(RasterIOPlugin, self).__init__(
            name='rasterio',
            version=__version__,
            container='xarray',
            partition_access=True)

    def open(self, urlpath, chunks, **kwargs):
        """
        Create RasterIOSource instance

        If `rasterio`_ is installed, additional file-types such as GeoTIFF are
        supported.

        .. rasterio: https://rasterio.readthedocs.io

        Parameters
        ----------
        urlpath: str
            Path to source file.
        chunks: int or dict
            Chunks is used to load the new dataset into dask
            arrays. Use ``chunks={}`` to load without chunking.
        kwargs:
            Further parameters are passed to xr.open_rasterio
        """
        from intake_xarray.raster import RasterIOSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return RasterIOSource(
            urlpath=urlpath,
            chunks=chunks,
            xarray_kwargs=source_kwargs,
            metadata=base_kwargs['metadata'])


class ZarrPlugin(base.Plugin):
    """zarr>xarray reader"""

    def __init__(self):
        super(ZarrPlugin, self).__init__(
            name='zarr',
            version=__version__,
            container='xarray',
            partition_access=True
        )

    def open(self, urlpath, storage_options=None, **kwargs):
        """
        Parameters
        ----------
        urlpath: str
            Path to source. This can be a local directory or a remote data
            service (i.e., with a protocol specifier like ``'s3://``).
        storage_options: dict
            Parameters passed to the backend file-system
        kwargs:
            Further parameters are passed to xr.open_zarr
        """
        from intake_xarray.xzarr import ZarrSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ZarrSource(urlpath, storage_options, base_kwargs['metadata'],
                          **source_kwargs)


class DataSourceMixin:
    """Common behaviours for plugins in this repo"""

    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
        if self._ds is None:
            self._open_dataset()

        metadata = {
            'dims': dict(self._ds.dims),
            'data_vars': tuple(self._ds.data_vars.keys()),
            'coords': tuple(self._ds.coords.keys())
        }
        metadata.update(self._ds.attrs)
        return base.Schema(
            datashape=None,
            dtype=self._ds,
            shape=None,
            npartitions=None,
            extra_metadata=metadata)

    def read(self):
        """Return a version of the xarray with all the data in memory"""
        self._load_metadata()
        return self._ds.load()

    def read_chunked(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def read_partition(self, i):
        """Fetch one chunk of data at tuple index i

        (not yet implemented)
        """
        from dask.delayed import Delayed
        import dask
        import numpy as np
        self._load_metadata()
        if isinstance(i, int):
            i = (i, )
        elif isinstance(i, list):
            i = tuple(i)
        if not isinstance(i, (tuple, list)):
            raise TypeError('For Xarray sources, must specify partition as '
                            'tuple')
        if hasattr(self._ds, 'variables'):  # is dataset?
            arr = self._ds[i[0]].data
            i = i[1:]
        else:
            arr = self._ds.data
        if isinstance(arr, np.ndarray):
            return arr
        key = (arr.name, ) + i
        d = Delayed(key, arr.dask)
        return dask.compute(d)[0]

    def to_dask(self):
        """Return xarray object where variables are dask arrays"""
        return self.read_chunked()

    def close(self):
        """Delete open file from memory"""
        self._ds = None
