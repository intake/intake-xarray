# -*- coding: utf-8 -*-
import xarray as xr
from intake.source.base import PatternMixin
from intake.source.utils import reverse_format
from .base import DataSourceMixin


class NetCDFSource(DataSourceMixin, PatternMixin):
    """Open a xarray file.

    Parameters
    ----------
    urlpath: str
        Path to source file. May include glob "*" characters, format
        pattern strings, or list.
        Some examples:
            - ``{{ CATALOG_DIR }}data/air.nc``
            - ``{{ CATALOG_DIR }}data/*.nc``
            - ``{{ CATALOG_DIR }}data/air_{year}.nc``
    chunks: int or dict
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    path_as_pattern: bool or str, optional
        Whether to treat the path as a pattern (ie. ``data_{field}.nc``)
        and create new coodinates in the output corresponding to pattern
        fields. If str, is treated as pattern to match on. Default is True.
    """
    name = 'netcdf'

    def __init__(self, urlpath, chunks, xarray_kwargs=None, metadata=None,
                 path_as_pattern=True, **kwargs):
        self.path_as_pattern = path_as_pattern
        self.urlpath = urlpath
        self.chunks = chunks
        self._kwargs = xarray_kwargs or kwargs
        self._ds = None
        super(NetCDFSource, self).__init__(metadata=metadata)

    def _open_dataset(self):
        url = self.urlpath
        kwargs = self._kwargs
        if "*" in url or isinstance(url, list):
            _open_dataset = xr.open_mfdataset
            if self.pattern:
                kwargs.update(preprocess=self._add_path_to_ds)
        else:
            _open_dataset = xr.open_dataset

        self._ds = _open_dataset(url, chunks=self.chunks, **kwargs)

    def _add_path_to_ds(self, ds):
        """Adding path info to a coord for a particular file
        """
        var = next(var for var in ds)
        new_coords = reverse_format(self.pattern, ds[var].encoding['source'])
        return ds.assign_coords(**new_coords)
