import xarray as xr
from intake.source import base
from dask.bytes.core import get_fs, infer_options, update_storage_options


class ZarrSource(base.DataSource):
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

    def __init__(self, urlpath, storage_options=None, metadata=None, **kwargs):
        super(ZarrSource, self).__init__(
              container='xarray', metadata=metadata)
        self.urlpath = urlpath
        self.storage_options = storage_options
        self.kwargs = kwargs
        self._ds = None

    def _open_dataset(self):
        urlpath, protocol, options = infer_options(self.urlpath)
        update_storage_options(options, self.storage_options)

        self._fs, _ = get_fs(protocol, options)
        self._mapper = get_mapper(protocol, self._fs, urlpath)
        self._ds = xr.open_zarr(self._mapper, **self.kwargs)

    def _get_schema(self):
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
        self._ds = None
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
