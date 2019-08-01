from .base import DataSourceMixin


class ZarrSource(DataSourceMixin):
    """Open a xarray dataset.

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
    name = 'zarr'

    def __init__(self, urlpath, storage_options=None, metadata=None, **kwargs):
        super(ZarrSource, self).__init__(metadata=metadata)
        self.urlpath = urlpath
        self.storage_options = storage_options or {}
        self.kwargs = kwargs
        self._ds = None

    def _open_dataset(self):
        import xarray as xr
        from fsspec import get_mapper

        self._mapper = get_mapper(self.urlpath, **self.storage_options)
        self._ds = xr.open_zarr(self._mapper, **self.kwargs)

    def close(self):
        super(ZarrSource, self).close()
        self._fs = None
        self._mapper = None
