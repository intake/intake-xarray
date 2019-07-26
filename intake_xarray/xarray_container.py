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
    """Filler function to put in the ``store()`` dask graphs"""
    return 0


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
    import dask
    s = ZarrSerialiser()
    try:
        attrs = ds.attrs.copy()
        ds.attrs.pop('_ARRAY_DIMENSIONS', None)  # zarr implementation detail
        x = ds.to_zarr(s, compute=False)
        x.dask = dict(x.dask)
        for k, v in x.dask.items():
            # replace the data writing funcs with no-op, so as not to waste
            # time on serialization, when all we want is metadata
            if isinstance(k, tuple) and k[0].startswith('store-'):
                x.dask[k] = (noop, ) + x.dask[k][1:]
        dask.compute(x, scheduler='threads')
    finally:
        ds.attrs = attrs
    return s


class RemoteXarray(RemoteSource):
    """
    An xarray data source on the server
    """
    name = 'remote-xarray'
    container = 'xarray'

    def __init__(self, url, headers, **kwargs):
        """
        Initialise local xarray, whose dask arrays contain tasks that pull data

        The matadata contains a key "internal", which is a result of running
        ``serialize_zarr_ds`` on the xarray on the server. It is a dict
        containing the metadata parts of the original dataset (i.e., the
        keys with names like ".z*"). This can be opened by xarray as-is, and
        will make a local xarray object. In ``._get_schema()``, the numpy
        parts (coordinates) are fetched and the dask-array parts (cariables)
        have their dask graphs redefined to tasks that fetch data from the
        server.
        """
        import xarray as xr
        super(RemoteXarray, self).__init__(url, headers, **kwargs)
        self._schema = None
        self._ds = xr.open_zarr(self.metadata['internal'])

    def _get_schema(self):
        """Reconstruct xarray arrays

        The schema returned is not very informative as a representation,
        this method fetches coordinates data and creates dask arrays.
        """
        import dask.array as da
        if self._schema is None:
            metadata = {
                'dims': dict(self._ds.dims),
                'data_vars': {k: list(self._ds[k].coords)
                              for k in self._ds.data_vars.keys()},
                'coords': tuple(self._ds.coords.keys()),
            }
            if getattr(self, 'on_server', False):
                metadata['internal'] = serialize_zarr_ds(self._ds)
            metadata.update(self._ds.attrs)
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata)
            # aparently can't replace coords in-place
            # we immediately fetch the values of coordinates
            # TODO: in the future, these could be functions from the metadata?
            self._ds = self._ds.assign_coords(**{c: self._get_partition((c, ))
                                                 for c in metadata['coords']})
            for var in list(self._ds.data_vars):
                # recreate dask arrays
                name = '-'.join(['remote-xarray', var, self._source_id])
                arr = self._ds[var].data
                chunks = arr.chunks
                nparts = (range(len(n)) for n in chunks)
                if self.metadata.get('array', False):
                    # original was an array, not dataset - no variable name
                    extra = ()
                else:
                    extra = (var, )
                dask = {
                    (name, ) + part: (get_partition, self.url, self.headers,
                                      self._source_id, self.container,
                                      extra + part)

                    for part in itertools.product(*nparts)
                }
                self._ds[var].data = da.Array(
                    dask,
                    name,
                    chunks,
                    dtype=arr.dtype,
                    shape=arr.shape)
            if self.metadata.get('array', False):
                self._ds = self._ds[self.metadata.get('array')]
        return self._schema

    def _get_partition(self, i):
        """
        The partition should look like ("var_name", int, int...), where the
        number of ints matches the number of coordinate axes in the named
        variable, and is between 0 and the number of chunks in each axis. For
        an array, as opposed to a dataset, omit the variable name.
        """
        return get_partition(self.url, self.headers, self._source_id,
                             self.container, i)

    def to_dask(self):
        self._get_schema()
        return self._ds

    def read_chunked(self):
        """The dask repr is the authoritative chunked version"""
        self._get_schema()
        return self._ds

    def read(self):
        self._get_schema()
        return self._ds.load()

    def close(self):
        self._ds = None
        self._schema = None

    @staticmethod
    def _persist(source, path, **kwargs):
        """Save data to a local zarr

        Uses
        http://xarray.pydata.org/en/stable/generated/xarray.Dataset.to_zarr.html
        """
        from intake_xarray import ZarrSource
        source.to_dask().to_zarr(path, **kwargs)
        return ZarrSource(path)

