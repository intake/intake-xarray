from intake.source import base
import xarray as xr
__version__ = '0.0.1'


class NetCDFPlugin(base.Plugin):
    """Plugin for NetCDF reader"""

    def __init__(self):
        super(NetCDFPlugin, self).__init__(name='netcdf',
                                           version=__version__,
                                           container='python',
                                           partition_access=True)

    def open(self, urlpath, **kwargs):
        """
        Create NetCDFSource instance

        Parameters
        ----------
        table, connection, qargs, partitions
            See ``NetCDFSource``.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return NetCDFSource(urlpath=urlpath,
                            xarray_kwargs = source_kwargs,
                            metadata=base_kwargs['metadata'])


class NetCDFSource(base.DataSource):
    """Open a NetCDF file with xarray.

    Parameters
    ----------
    param: type
        description
    """
    container = 'python'

    def __init__(self, urlpath, xarray_kwargs=None, metadata=None):
        self.urlpath = urlpath
        self._kwargs = xarray_kwargs or {}
        self.chunks = self._kwargs.get('chunks') 
        self._ds = None
        super(NetCDFSource, self).__init__(
            container=self.container,
            metadata=metadata)

    def _open_dataset(self):
        return xr.open_dataset(self.urlpath, chunks=self.chunks)

    def _get_schema(self):
        if self._ds is None:
            self._ds = self._open_dataset()

        metadata = {
                'dims' : dict(self._ds.dims),
                'data_vars': tuple(self._ds.data_vars.keys()),
                'coords': tuple(self._ds.coords.keys())
            }
        metadata.update(self._ds.attrs)
        return base.Schema(
            datashape=None,
            dtype=xr.Dataset,
            shape=None,
            npartitions=None,
            extra_metadata=metadata
        )

    def read(self):
        self._load_metadata()
        return self._ds

    def read_chunked(self):
        raise Exception('read_chunked not supported for xarray containers.')

    def read_partition(self, i):
        raise Exception('read_partition not supported for xarray containers.')

    def to_dask(self):
        self._load_metadata()
        return self._ds.to_dask_dataframe()

    def close(self):
        self._ds.close()
