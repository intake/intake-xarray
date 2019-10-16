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
    auth: None, "esgf" or "urs"
        Method of authenticating to the OPeNDAP server.
        Choose from one of the following:
        'esgf' - [Default] Earth System Grid Federation.
        'urs' - NASA Earthdata Login, also known as URS.
        None - No authentication.
    """
    name = 'opendap'

    def __init__(self, urlpath, chunks, auth="esgf", xarray_kwargs=None, metadata=None,
                 **kwargs):
        self.urlpath = urlpath
        self.chunks = chunks
        self.auth = auth
        self._kwargs = xarray_kwargs or kwargs
        self._ds = None
        super(OpenDapSource, self).__init__(metadata=metadata)

    def _get_session(self):
        if self.auth is None:
            session = None
        else:
            if self.auth == "esgf":
                from pydap.cas.esgf import setup_session
            elif self.auth == "urs":
                from pydap.cas.urs import setup_session
            else:
                raise ValueError(
                    f"Authentication method should either be 'esgf' or 'urs', got {auth} instead."
                )
            username = os.getenv('DAP_USER', None)
            password = os.getenv('DAP_PASSWORD', None)
            session = setup_session(username, password, check_url=self.urlpath)

        return session

    def _open_dataset(self):
        import xarray as xr
        session = self._get_session()

        store = xr.backends.PydapDataStore.open(self.urlpath, session=session)
        self._ds = xr.open_dataset(store, chunks=self.chunks, **self._kwargs)
