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

    def reader(self, filename, **kwargs):
        import xarray as xr

        session = _get_session(filename)

        store = xr.backends.PydapDataStore.open(filename, session=session)
        return xr.open_dataset(store, **kwargs)

    def multireader(self, filename, **kwargs):
        import xarray as xr

        session = _get_session(filename)

        store = xr.backends.PydapDataStore.open(filename, session=session)
        return xr.open_mfdataset(store, **kwargs)
