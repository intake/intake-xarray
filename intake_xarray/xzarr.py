import xarray as xr
from dask.bytes.core import get_fs, infer_options, update_storage_options
from .base import XarraySource


class ZarrSource(XarraySource):
    """Open a zarr file as an xarray dataset.

    Parameters
    ----------
    urlpath: str
        Path to source. This can be a local directory or a remote data
        service (i.e., with a protocol specifier like ``'s3://``).
    xarray_kwargs:
        Further parameters are passed to ``xr.open_zarr``
    """
    name = 'zarr'
    __doc__ += XarraySource.__inheritted_parameters_doc__

    def __init__(self, urlpath, chunks=None, **kwargs):
        super(ZarrSource, self).__init__(urlpath, chunks, **kwargs)

    def reader(self, filename, chunks=None, **kwargs):
        urlpath, protocol, options = infer_options(self.urlpath)
        update_storage_options(options, self.storage_options)

        self._fs, _ = get_fs(protocol, options)
        if protocol != 'file':
            self._mapper = get_mapper(protocol, self._fs, urlpath)
            return xr.open_zarr(self._mapper, **kwargs)
        else:
            return xr.open_zarr(self.urlpath, **kwargs)

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
