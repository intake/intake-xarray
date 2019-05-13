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
        self.storage_options = storage_options
        self.kwargs = kwargs
        self._ds = None

    def _open_dataset(self):
        import xarray as xr
        from dask.bytes.core import get_fs, infer_options, \
            update_storage_options
        urlpath, protocol, options = infer_options(self.urlpath)
        update_storage_options(options, self.storage_options)

        self._fs, _ = get_fs(protocol, options)
        if protocol != 'file':
            self._mapper = get_mapper(protocol, self._fs, urlpath)
            self._ds = xr.open_zarr(self._mapper, **self.kwargs)
        else:
            self._ds = xr.open_zarr(self.urlpath, **self.kwargs)

    def close(self):
        super(ZarrSource, self).close()
        self._fs = None
        self._mapper = None


def get_mapper(protocol, fs, path):
    if protocol == 's3':
        from s3fs.mapping import S3Map
        return S3Map(path, fs)
    elif protocol == 'gcs':
        from gcsfs.mapping import GCSMap
        return GCSMap(path, fs)
    else:
        raise NotImplementedError
