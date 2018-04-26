import xarray as xr
from intake.source import base


class NetCDFSource(base.DataSource):
    """Open a xarray file.

    Parameters
    ----------
    urlpath: str
        Path to source file. May include glob "*" characters. Must be a
        location in the local file-system.
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
        super(NetCDFSource, self).__init__(container='xarray',
                                           metadata=metadata)

    def _open_dataset(self):
        url = self.urlpath
        if "*" in url:
            return xr.open_mfdataset(url, chunks=self.chunks, **self._kwargs)
        else:
            return xr.open_dataset(url, chunks=self.chunks, **self._kwargs)

    def _get_schema(self):
        if self._ds is None:
            self._ds = self._open_dataset()

        metadata = {
            'dims': dict(self._ds.dims),
            'data_vars': tuple(self._ds.data_vars.keys()),
            'coords': tuple(self._ds.coords.keys())
        }
        metadata.update(self._ds.attrs)
        return base.Schema(
            datashape=None,
            dtype=xr.Dataset,
            shape=None,
            npartitions=None,
            extra_metadata=metadata)

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
