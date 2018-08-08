import itertools
import os
from intake.container.base import RemoteSource, get_partition
from intake.source.base import Schema


class ZarrSerialiser(dict):
    """Keeps only the non-data parts of a Zarr dataset for serialization


    In this example ``s`` is safe to serialize with msgpack for sending
    across the wire. The reconstituted dataset will contain numpy and dask
    arrays that have no data.
    """

    def __setitem__(self, item, value):
        if os.path.basename(item) in ['.zgroup', '.zattrs', '.zarray']:
            dict.__setitem__(self, item, value)


def noop(*args):
    return None


def serialize_zarr_ds(ds):
    """Gather group/metadata information from a Zarr into dictionary repr

    A version of the dataset can be recreated, but will not be able to directly
    load data without further manipulation.

    Use as follows
    >>> out = serialize_zarr(s._ds)

    and reconstitute with
    >>> d2 = xr.open_zarr(out, decode_times=False)

    (decode_times is required here because the times will be random binary
    data and not be decodable)

    Parameters
    ----------
    ds: xarray dataset

    Returns
    -------
    dictionary with .z* keys for the various elements of the original dataset.
    """
    import dask.array as da
    s = ZarrSerialiser()
    safe = {}
    try:
        attrs = ds.attrs.copy()
        ds.attrs.pop('_ARRAY_DIMENSIONS', None)  # zarr implementation detail
        for name in ds.variables:
            if not isinstance(ds[name].data, da.Array):
                continue
            d = ds[name].data
            safe[name] = d.dask
            d.dask = {k: ((noop,) if isinstance(k, tuple) else d.dask[k]) for
                      k, v in d.dask.items()}
        ds.to_zarr(s)
    finally:
        ds.attrs = attrs
        for name, dask in safe.items():
            ds[name].data.dask = dask
    return s


class RemoteXarray(RemoteSource):
    name = 'remote-xarray'
    container = 'xarray'

    def __init__(self, url, headers, **kwargs):
        import xarray as xr
        super(RemoteXarray, self).__init__(url, headers, **kwargs)
        self._schema = None
        self._ds = xr.open_zarr(self.metadata['internal'])

    def _get_schema(self):
        import dask.array as da
        if self._schema is None:
            metadata = {
                'dims': dict(self._ds.dims),
                'data_vars': {k: list(self._ds[k].coords)
                              for k in self._ds.data_vars.keys()},
                'coords': tuple(self._ds.coords.keys()),
                'internal': serialize_zarr_ds(self._ds)
            }
            metadata.update(self._ds.attrs)
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata)
            # aparently can't replace coords in-place
            self._ds = self._ds.assign_coords(**{c: self._get_partition((c, ))
                                                 for c in metadata['coords']})
            if hasattr(self._ds, 'variables'):  # is DataSet?
                for var in list(self._ds.data_vars):
                    name = '-'.join(['remote-array', var, self._source_id])
                    arr = self._ds[var].data
                    chunks = arr.chunks
                    nparts = (range(len(n)) for n in chunks)
                    dask = {
                        (name, ) + part: (get_partition, self.url, self.headers,
                                          self._source_id, self.container,
                                          (var, ) + part)

                        for part in itertools.product(*nparts)
                    }
                    self._ds[var].data = da.Array(dask, name, chunks,
                                                  arr.dtype, arr.shape)
            else:
                raise NotImplementedError  # DataArray

        return self._schema

    def _get_partition(self, i):
        return get_partition(self.url, self.headers, self._source_id,
                             self.container, i)

    def to_dask(self):
        self._get_schema()
        return self._ds

    def read_chunked(self):
        self._get_schema()
        return self._ds

    def close(self):
        self._ds = None
        self._schema = None
