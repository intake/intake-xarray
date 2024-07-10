# -*- coding: utf-8 -*-
import requests
import os

from intake import readers
from intake_xarray.base import IntakeXarraySourceAdapter

class OpenDapSource(IntakeXarraySourceAdapter):
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

    def __init__(self, urlpath, chunks=None, engine="pydap", xarray_kwargs=None, metadata=None,
                 **kwargs):
        data = readers.datatypes.OpenDAP(urlpath)
        self.reader = readers.XArrayDatasetReader(
            data, engine=engine, **(xarray_kwargs or {}), metadata=metadata, **kwargs
        )
