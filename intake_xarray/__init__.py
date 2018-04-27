# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright 2016 Continuum Analytics, Inc.
#
# May be copied and distributed freely only as part of an Anaconda or
# Miniconda installation.
# -----------------------------------------------------------------------------
from intake.source import base
import xarray as xr
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class NetCDFPlugin(base.Plugin):
    """Plugin for netcdf->xarray reader"""

    def __init__(self):
        super(NetCDFPlugin, self).__init__(
            name='netcdf',
            version=__version__,
            container='xarray',
            partition_access=True)

    def open(self, urlpath, chunks, **kwargs):
        """
        Create NetCDFSource instance

        Parameters
        ----------
        urlpath: str
            Path to source file(s).
        chunks: int or dict
            Chunks is used to load the new dataset into dask
            arrays. ``chunks={}`` loads the dataset with dask using a single
            chunk for all arrays.
        """
        from intake_xarray.netcdf import NetCDFSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return NetCDFSource(
            urlpath=urlpath,
            chunks=chunks,
            xarray_kwargs=source_kwargs,
            metadata=base_kwargs['metadata'])


class ZarrPlugin(base.Plugin):
    """zarr>xarray reader"""

    def __init__(self):
        super(ZarrPlugin, self).__init__(
            name='zarr',
            version=__version__,
            container='xarray',
            partition_access=True
        )

    def open(self, urlpath, storage_options=None, **kwargs):
        """
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
        from intake_xarray.xzarr import ZarrSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ZarrSource(urlpath, storage_options, base_kwargs['metadata'],
                          **source_kwargs)


class DataSourceMixin:

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