# -*- coding: utf-8 -*-
from .base import DataSourceMixin

import requests
import os


def _create_generic_http_auth_session(username, password, check_url=None):
    if username is None or password is None:
        raise Exception("To use HTTP auth with the OPeNDAP driver you "
                        "need to set the DAP_USER and DAP_PASSWORD "
                        "environment variables")
    session = requests.Session()
    session.auth = (username, password)
    return session


class OpenDapSource(DataSourceMixin):
    """Open a OPeNDAP source.

    Parameters
    ----------
    urlpath: str
        Path to source file.
    chunks: None, int or dict
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    auth: None, "esgf" or "urs"
        Method of authenticating to the OPeNDAP server.
        Choose from one of the following:
        None - [Default] Anonymous access.
        'esgf' - Earth System Grid Federation.
        'urs' - NASA Earthdata Login, also known as URS.
        'generic_http' - OPeNDAP servers which support plain HTTP authentication
        None - No authentication.
        Note that you will need to set your username and password respectively using the
        environment variables DAP_USER and DAP_PASSWORD.
    engine: str
        Engine used for reading OPeNDAP URL. Should be one of 'pydap' or 'netcdf4'.
    """
    name = 'opendap'

    def __init__(self, urlpath, chunks=None, auth=None, engine="pydap", xarray_kwargs=None, metadata=None,
                 **kwargs):
        self.urlpath = urlpath
        self.chunks = chunks
        self.auth = auth
        self.engine = engine
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
            elif self.auth == "generic_http":
                setup_session = _create_generic_http_auth_session
            else:
                raise ValueError(
                    "Authentication method should either be None, 'esgf', 'urs' or "
                    f"'generic_http', got '{self.auth}' instead."
                )
            username = os.getenv('DAP_USER', None)
            password = os.getenv('DAP_PASSWORD', None)
            session = setup_session(username, password, check_url=self.urlpath)

        return session

    def _get_store(self):
        import xarray as xr
        session = self._get_session()
        if self.engine == "netcdf4":
            if session:
                raise ValueError(
                    "Opendap session requires 'pydap' engine."
                )
            return xr.backends.NetCDF4DataStore.open(self.urlpath)
        elif self.engine == "pydap":
            return xr.backends.PydapDataStore.open(self.urlpath, session=session)
        else:
            raise ValueError(
                "xarray engine for opendap driver should either be 'netcdf4' or 'pydap'."
            )

    def _open_dataset(self):
        import xarray as xr
        store = self._get_store()
        self._ds = xr.open_dataset(store, chunks=self.chunks, **self._kwargs)
