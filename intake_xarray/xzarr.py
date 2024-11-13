from intake import readers

from intake_xarray.base import IntakeXarraySourceAdapter


class ZarrSource(IntakeXarraySourceAdapter):
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
        data = readers.datatypes.Zarr(urlpath, storage_options=storage_options,
                                      metadata=metadata)
        self.reader = readers.XArrayDatasetReader(data, **kwargs)
