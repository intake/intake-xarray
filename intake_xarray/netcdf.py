# -*- coding: utf-8 -*-
from intake import readers

from intake_xarray.base import IntakeXarraySourceAdapter


class NetCDFSource(IntakeXarraySourceAdapter):
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

    def __init__(self, urlpath,
                 xarray_kwargs=None, metadata=None,
                 path_as_pattern=True, storage_options=None, **kwargs):
        data = readers.datatypes.NetCDF3(urlpath, storage_options=storage_options,
                                      metadata=metadata)
        if (path_as_pattern is True and "{" in urlpath) or isinstance(path_as_pattern, str):
            reader = readers.XArrayPatternReader(data, **(xarray_kwargs or {}), metadata=metadata,
                                                 pattern=path_as_pattern, **kwargs)
        else:
            reader = readers.XArrayDatasetReader(data, **(xarray_kwargs or {}), metadata=metadata, **kwargs)
        self.reader = reader
