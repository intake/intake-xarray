from . import __version__
from intake.source.base import DataSource, Schema, PatternMixin
from intake.source.utils import reverse_formats

import glob
import numpy as np

try:
    import xarray as xr
except (AttributeError, ImportError):
    raise ImportError('xarray must be installed to use intake-xarray plugin')


class XarraySource(DataSource, PatternMixin):
    """
    Open a file as an xarray object. It is often preferable to use
    the plugin matching your file format.

    Parameters
    ----------
    urlpath : str or iterable
        Path to source file(s). May include glob "*" characters or format
        pattern strings. Must be a format supported by reader and depending
        on the reader might be required to be local. All readers accept files
        from cache.

        Some examples:
            - ``{{ CATALOG_DIR }}data/RGB.tif``
            - ``s3://data/*.nc``
            - ``http://thredds.ucar.edu/thredds/dodsC/grib/FNMOC/WW3/Global_1p0deg/Best``
            - ``https://github.com/pydata/xarray-data/blob/master/air_temperature.nc?raw=true``
            - ``./data/landsat8_{collection_date:%Y%m%d}_band{band}.tif``
            - ``s3://data/Images/{{ landuse }}/{{ landuse }}{{ '%02d' % id }}.tif``
    reader : function, optional
        Reader function which takes filename, chunks and any kwargs in
        ``xarray_kwargs``; returns an xarray object. Default is
        ``xarray.open_dataset``:
        http://xarray.pydata.org/en/stable/generated/xarray.open_dataset.html
    multireader : function, optional
        Reader function which takes a glob representing filenames, chunks
        and any kwargs in ``xarray_kwargs``. Returns an xarray object. This
        reader is only used when glob is not a pattern. Default is
        ``xarray.open_mfdataset``:
        http://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html
    xarray_kwargs : dict, optional
        Any further arguments to pass to ``reader``.
    """
    version = __version__
    container = 'xarray'
    partition_access = True
    name = 'xarray'

    __inheritted_parameters_doc__ = """
    chunks : int or dict, optional
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for each file.
    concat_dim : str, optional
        When using glob or list of files, dimension on which to concatenate the
        data from the files. Default: 'concat_dim'
    merge_dim : str, opional
        When using glob or list of files, dimension on which to merge the
        data from the files. Each file will result in a data_var. Supercedes
        concat_dim unless urlpath is a pattern in which case both can be used.
    path_as_pattern : bool or str, optional
        Whether to treat the path as a pattern (ie. ``data_{field}.tif``)
        and create new coodinates in the output corresponding to pattern
        fields. If str, is treated as pattern to match on. Default is True.
    storage_options : dict, optional
        Any parameters that need to be passed to the remote data backend,
        such as credentials. For public data on s3 set ``{'anon': True}``.
    metadata : dict, optional
        Any additional metadata to include in source.
    **kwargs :
        Additional arguments to pass into reader. Only use this or xarray_kwargs.
    """
    __doc__ += __inheritted_parameters_doc__

    def __init__(self, urlpath, chunks=None,
                 concat_dim='concat_dim', merge_dim=None,
                 xarray_kwargs=None, metadata=None,
                 path_as_pattern=True, storage_options=None,
                 reader=None, multireader=None, **kwargs):
        self.path_as_pattern = path_as_pattern
        self.urlpath = urlpath
        self.chunks = chunks
        self.concat_dim = concat_dim
        self.merge_dim = merge_dim
        self.kwargs = xarray_kwargs or kwargs
        self._ds = None
        self.storage_options = storage_options
        self._reader = reader or xr.open_dataset
        self._multireader = multireader or xr.open_mfdataset
        super(XarraySource, self).__init__(metadata=metadata)

    def reader(self, filename, chunks, **kwargs):
        if getattr(self, '_reader', None) is not None:
            return self._reader(filename, chunks, **kwargs)
        raise NotImplementedError('Plugin must implement a reader function '
                                  'which takes at least filename, and chunks '
                                  'and returns an xarray object')

    def multireader(self, filename, chunks, **kwargs):
        if getattr(self, '_multireader', None) is not None:
            return self._multireader(filename, chunks, **kwargs)
        raise NotImplementedError('Plugin must implement a multireader function '
                                  'or set this to None. The function takes a glob '
                                  'representing filenames, chunks, and arbitrary '
                                  'kwargs and returns an xarray object')

    def _get_coords(self, das, field_values, index=None):
        """ Given a list of dask xarray objects and a dict of arrays of coords
            return a dict of coords that should be set on the concatenated data
        """
        return {
            k: xr.concat(
                [xr.DataArray(
                    np.full(das[i].sizes.get(self.concat_dim, 1), v),
                    dims=self.concat_dim
                ) for i, v in enumerate(values) if index is None or i in index],
                dim=self.concat_dim)
            for k, values in field_values.items() if k != self.merge_dim
        }

    def _open_files(self, files):
        das = [self.reader(f, chunks=self.chunks, **self.kwargs)
               for f in files]
        if not self.pattern:
            if self.merge_dim:
                for i, da in enumerate(das):
                    if isinstance(da, xr.DataArray):
                        da.name = '{dim}_{i}'.format(dim=self.merge_dim, i=i)
                return xr.merge(das)
            if self.concat_dim:
                return xr.concat(das, dim=self.concat_dim)

        field_values = reverse_formats(self.pattern, files)

        if not self.merge_dim:
            coords = self._get_coords(das, field_values)
            out = xr.concat(das, dim=self.concat_dim)
            return out.assign_coords(**coords).chunk(self.chunks)

        data_vars = set(field_values.get(self.merge_dim, []))
        if len(data_vars) == 0:
            data_vars = ['{}_{}'.format(self.merge_dim, i) for i in range(len(das))]

        merge_das = []
        for data_var in data_vars:
            idxs = [i for i, item in enumerate(
                field_values.get(self.merge_dim, [])) if item == data_var]
            if len(idxs) > 1:
                data = [das[i] for i in idxs]
                coords = self._get_coords(das, field_values, idxs)
                da = xr.concat(data, dim=self.concat_dim)
                da = da.assign_coords(**coords).chunk(self.chunks)
            else:
                if len(idxs) == 1:
                    i = idxs[0]
                else:
                    i = data_vars.index(data_var)
                da = das[i]
                coords = {k: values[i] for k, values in field_values.items() if k != self.merge_dim}
                da = da.assign_coords(**coords).chunk(self.chunks)
            if isinstance(da, xr.DataArray):
                da.name = data_var
            merge_das.append(da)

        return xr.merge(merge_das)

    def _open_dataset(self):
        is_glob = '*' in self.urlpath
        is_list = isinstance(self.urlpath, list)

        if self.multireader and not (self.pattern or self.merge_dim) and is_glob:
            self._ds = self.multireader(self.urlpath, chunks=self.chunks,
                                        concat_dim=self.concat_dim, **self.kwargs)
        elif is_glob:
            files = sorted(glob.glob(self.urlpath))
            if len(files) == 0:
                raise Exception("No files found at {}".format(self.urlpath))
            self._ds = self._open_files(files)
        elif is_list:
            self._ds = self._open_files(self.urlpath)
        else:
            self._ds = self.reader(self.urlpath, chunks=self.chunks,
                                   **self.kwargs)

    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
        from .xarray_container import serialize_zarr_ds

        is_multi = '*' in self.urlpath or isinstance(self.urlpath, list)
        self.urlpath = self._get_cache(self.urlpath)[0]
        if isinstance(self.urlpath, list) and not is_multi:
            self.urlpath = self.urlpath[0]

        if self._ds is None:
            self._open_dataset()

            if isinstance(self._ds, xr.DataArray):
                da = self._ds
                ds = xr.Dataset({'value': da})
                extra_metadata = {'array': 'value'}
            else:
                ds = self._ds
                da = ds[list(ds.data_vars)[0]]
                extra_metadata = {}

            metadata = {
                'dims': dict(ds.dims),
                'data_vars': {k: list(ds[k].coords)
                              for k in ds.data_vars.keys()},
                'coords': tuple(ds.coords.keys()),
                **extra_metadata
            }
            if getattr(self, 'on_server', False):
                metadata['internal'] = serialize_zarr_ds(ds)

            self._schema = Schema(
                datashape=None,
                dtype=str(da.dtype),
                shape=da.shape,
                npartitions=getattr(da.data, 'npartitions', None),
                extra_metadata=metadata)

        return self._schema

    def read(self):
        """Return a version of the xarray with all the data in memory"""
        self._load_metadata()
        return self._ds.load()

    def read_chunked(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def read_partition(self, i):
        """Fetch one chunk of data at tuple index i
        """
        import numpy as np
        self._load_metadata()
        if not isinstance(i, (tuple, list)):
            raise TypeError('For Xarray sources, must specify partition as '
                            'tuple')
        if isinstance(i, list):
            i = tuple(i)
        if hasattr(self._ds, 'variables') or i[0] in self._ds.coords:
            arr = self._ds[i[0]].data
            i = i[1:]
        else:
            arr = self._ds.data
        if isinstance(arr, np.ndarray):
            return arr
        # dask array
        return arr.blocks[i].compute()

    def to_dask(self):
        """Return xarray object where variables are dask arrays"""
        return self.read_chunked()

    def close(self):
        """Delete open file from memory"""
        self._ds = None
        self._schema = None
