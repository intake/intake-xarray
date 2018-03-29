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

    def open(self, urlpath, chunks, **kwargs):
        """
        Create NetCDFSource instance

        Parameters
        ----------
        urlpath: str
            Path to source file.
        chunks: int or dict
            Chunks is used to load the new dataset into dask
            arrays. ``chunks={}`` loads the dataset with dask using a single
            chunk for all arrays.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return NetCDFSource(urlpath=urlpath,
                            chunks=chunks,
                            xarray_kwargs = source_kwargs,
                            metadata=base_kwargs['metadata'])


class NetCDFSource(base.DataSource):
    """Open a NetCDF file with xarray.

    Parameters
    ----------
    urlpath: str
        Path to source file.
    chunks: int or dict
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    """

    def __init__(self, urlpath, chunks, xarray_kwargs=None, metadata=None):
        self.urlpath = urlpath
        self.chunks = chunks
        self._kwargs = xarray_kwargs or {}
        self._ds = None
        super(NetCDFSource, self).__init__(
            container=None,
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
        return self._ds.load()

    def read_chunked(self):
        self._load_metadata()
        return self._ds

    def read_partition(self, i):
        raise NotImplementedError

    def to_dask(self):
        return self.read_chunked()

    def close(self):
        self._ds.close()
