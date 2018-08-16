from . import __version__
from intake.source.base import DataSource, Schema


class DataSourceMixin(DataSource):
    """Common behaviours for plugins in this repo"""
    version = __version__
    container = 'xarray'
    partition_access = True

    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""

        self.urlpath, *_ = self._get_cache(self.urlpath)

        if self._ds is None:
            self._open_dataset()

        metadata = {
            'dims': dict(self._ds.dims),
            'data_vars': tuple(self._ds.data_vars.keys()),
            'coords': tuple(self._ds.coords.keys())
        }
        metadata.update(self._ds.attrs)
        return Schema(
            datashape=None,
            dtype=self._ds,
            shape=None,
            npartitions=None,
            extra_metadata=metadata)

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

        (not yet implemented)
        """
        from dask.delayed import Delayed
        import dask
        import numpy as np
        self._load_metadata()
        if not isinstance(i, (tuple, list)):
            raise TypeError('For Xarray sources, must specify partition as '
                            'tuple')
        if hasattr(self._ds, 'variables'):  # is dataset?
            arr = self._ds[i[0]].data
            i = i[1:]
        else:
            arr = self._ds.data
        if isinstance(arr, np.ndarray):
            return arr
        key = (arr.name, ) + i
        d = Delayed(key, arr.dask)
        return dask.compute(d)[0]

    def to_dask(self):
        """Return xarray object where variables are dask arrays"""
        return self.read_chunked()

    def close(self):
        """Delete open file from memory"""
        self._ds = None
