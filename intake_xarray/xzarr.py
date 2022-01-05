from .base import DataSourceMixin


class ZarrSource(DataSourceMixin):
    """Open a xarray dataset.

    If the path is passed as a list or a string containing "*", then multifile open
    will be called automatically.

    Note that the implicit default value of the ``chunks`` kwarg is ``{}``, i.e., dask
    will be used to open the dataset with chunksize as inherent in the file. To bypass
    dask (if you only want to use ``.read()``), use ``chunks=None``.

    Parameters
    ----------
    urlpath: str
        Path to source. This can be a local directory or a remote data
        service (i.e., with a protocol specifier like ``'s3://``).
    storage_options: dict
        Parameters passed to the backend file-system
    kwargs:
        Further parameters are passed to xarray
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
        kw = self.kwargs.copy()
        if "consolidated" not in kw:
            kw['consolidated'] = False
        if "chunks" not in kw:
            kw["chunks"] = {}
        kw["engine"] = "zarr"
        if self.storage_options and "storage_options" not in kw.get("backend_kwargs", {}):
            kw.setdefault("backend_kwargs", {})["storage_options"] = self.storage_options
        if isinstance(self.urlpath, list) or "*" in self.urlpath:
            self._ds = xr.open_mfdataset(self.urlpath, **kw)
        else:
            self._ds = xr.open_dataset(self.urlpath, **kw)

    def close(self):
        super(ZarrSource, self).close()
        self._fs = None
        self._mapper = None
