# -*- coding: utf-8 -*-
from .base import XarraySource


def _get_session(filename):
    import os
    from pydap.cas.esgf import setup_session

    username = os.getenv('DAP_USER', None)
    password = os.getenv('DAP_PASSWORD', None)
    return setup_session(
        username,
        password,
        check_url=filename)


def _get_store(filename):
    from xarray.backends import PydapDataStore

    session = _get_session(filename)
    return PydapDataStore.open(filename, session=session)


def reader(filename, **kwargs):
    from xarray import open_dataset

    return open_dataset(_get_store(filename), **kwargs)


def multireader(filename, **kwargs):
    from xarray import open_mfdataset

    return open_mfdataset(_get_store(filename), **kwargs)


class OpenDapSource(XarraySource):
    """Open an authenticated OPeNDAP source.

    NOTE: For an un-authenticated OPeNDAP source use 'netcdf' instead.

    Parameters
    ----------
    urlpath : str
        Path to source file.
    xarray_kwargs : dict, optional
        Any further arguments to pass to ``xr.open_dataset``.
    """
    name = 'opendap'
    __doc__ += XarraySource.__inheritted_parameters_doc__

    def __init__(self, urlpath, chunks=None, **kwargs):
        super(OpenDapSource, self).__init__(urlpath, chunks, **kwargs)
        self.reader = reader
        self.multireader = multireader
