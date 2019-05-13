# -*- coding: utf-8 -*-
from .base import DataSourceMixin

import os


class OpenDapSource(DataSourceMixin):
    """Open a OPeNDAP source.

    Parameters
    ----------
    urlpath: str
        Path to source file.
    chunks: int or dict
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    """
    name = 'opendap'

    def __init__(self, urlpath, chunks, xarray_kwargs=None, metadata=None,
                 **kwargs):
        self.urlpath = urlpath
        self.chunks = chunks
        self._kwargs = xarray_kwargs or kwargs
        self._ds = None
        super(OpenDapSource, self).__init__(metadata=metadata)

    def _get_session(self):
        from pydap.cas.esgf import setup_session
        username = os.getenv('DAP_USER', None)
        password = os.getenv('DAP_PASSWORD', None)
        return setup_session(
            username,
            password,
            check_url=self.urlpath)

    def _open_dataset(self):
        import xarray as xr
        session = self._get_session()

        store = xr.backends.PydapDataStore.open(self.urlpath, session=session)
        self._ds = xr.open_dataset(store, chunks=self.chunks, **self._kwargs)
