
# TODO: write RemoteXarray class
# should reuse dask array assembly in the main Intake code-base
import os


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
        ds.attrs.pop('_ARRAY_DIMENSIONS')
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
