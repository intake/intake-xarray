# -*- coding: utf-8 -*-
import xarray as xr
from intake.source import base
from . import DataSourceMixin

class NetCDFSource(DataSourceMixin, base.DataSource):
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
        super(NetCDFSource, self).__init__(
            container='xarray', metadata=metadata)

    def _open_dataset(self):
        url = self.urlpath
        _open_dataset = xr.open_mfdataset if "*" in url else xr.open_dataset

        self._ds = _open_dataset(url, chunks=self.chunks, **self._kwargs)
