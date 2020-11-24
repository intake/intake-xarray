# -*- coding: utf-8 -*-
import fsspec
from distutils.version import LooseVersion
from intake.source.base import PatternMixin
from intake.source.utils import reverse_format
from .base import DataSourceMixin


class NetCDFSource(DataSourceMixin, PatternMixin):
    """Open a xarray file.

    Parameters
    ----------
    urlpath : str, List[str]
        Path to source file. May include glob "*" characters, format
        pattern strings, or list.
        Some examples:
            - ``{{ CATALOG_DIR }}/data/air.nc``
            - ``{{ CATALOG_DIR }}/data/*.nc``
            - ``{{ CATALOG_DIR }}/data/air_{year}.nc``
    chunks : int or dict, optional
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    combine : ({'by_coords', 'nested'}, optional)
        Which function is used to concatenate all the files when urlpath
        has a wildcard. It is recommended to set this argument in all
        your catalogs because the default has changed and is going to change.
        It was "nested", and is now the default of xarray.open_mfdataset
        which is "auto_combine", and is planed to change from "auto" to
        "by_corrds" in a near future.
    concat_dim : str, optional
        Name of dimension along which to concatenate the files. Can
        be new or pre-existing if combine is "nested". Must be None or new if
        combine is "by_coords".
    path_as_pattern : bool or str, optional
        Whether to treat the path as a pattern (ie. ``data_{field}.nc``)
        and create new coodinates in the output corresponding to pattern
        fields. If str, is treated as pattern to match on. Default is True.
    xarray_kwargs: dict
        Additional xarray kwargs for xr.open_dataset() or xr.open_mfdataset().
    storage_options: dict
        If using a remote fs (whether caching locally or not), these are
        the kwargs to pass to that FS.
    """
    name = 'netcdf'

    def __init__(self, urlpath, chunks=None, combine=None, concat_dim=None,
                 xarray_kwargs=None, metadata=None,
                 path_as_pattern=True, storage_options=None, **kwargs):
        self.path_as_pattern = path_as_pattern
        self.urlpath = urlpath
        self.chunks = chunks
        self.concat_dim = concat_dim
        self.combine = combine
        self.storage_options = storage_options or {}
        self.xarray_kwargs = xarray_kwargs or {}
        self._ds = None
        if isinstance(self.urlpath, list):
            self._can_be_local = fsspec.utils.can_be_local(self.urlpath[0])
        else:
            self._can_be_local = fsspec.utils.can_be_local(self.urlpath)
        super(NetCDFSource, self).__init__(metadata=metadata, **kwargs)

    def _open_dataset(self):
        import xarray as xr
        url = self.urlpath

        kwargs = self.xarray_kwargs

        if "*" in url or isinstance(url, list):
            _open_dataset = xr.open_mfdataset
            if self.pattern:
                kwargs.update(preprocess=self._add_path_to_ds)
            if self.combine is not None:
                if 'combine' in kwargs:
                    raise Exception("Setting 'combine' argument twice  in the catalog is invalid")
                kwargs.update(combine=self.combine)
            if self.concat_dim is not None:
                if 'concat_dim' in kwargs:
                    raise Exception("Setting 'concat_dim' argument twice  in the catalog is invalid")
                kwargs.update(concat_dim=self.concat_dim)
        else:
            _open_dataset = xr.open_dataset

        if self._can_be_local:
            url = fsspec.open_local(self.urlpath, **self.storage_options)
        else:
            # https://github.com/intake/filesystem_spec/issues/476#issuecomment-732372918
            url = fsspec.open(self.urlpath, **self.storage_options).open()

        self._ds = _open_dataset(url, chunks=self.chunks, **kwargs)

    def _add_path_to_ds(self, ds):
        """Adding path info to a coord for a particular file
        """
        import xarray as xr
        XARRAY_VERSION = LooseVersion(xr.__version__)
        if not (XARRAY_VERSION > '0.11.1'):
            raise ImportError("Your version of xarray is '{}'. "
                "The insurance that source path is available on output of "
                "open_dataset was added in 0.11.2, so "
                "pattern urlpaths are not supported.".format(XARRAY_VERSION))

        var = next(var for var in ds)
        new_coords = reverse_format(self.pattern, ds[var].encoding['source'])
        return ds.assign_coords(**new_coords)
