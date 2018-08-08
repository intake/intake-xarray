import xarray as xr
from intake.source import base
from .base import DataSourceMixin, Schema


class RasterIOSource(DataSourceMixin):
    """Open a xarray dataset via RasterIO.

    This creates an xarray.array, not a dataset (i.e., there is exactly one
    variable).

    See https://rasterio.readthedocs.io/en/latest/ for the file formats
    supported, particularly GeoTIFF, and
    http://xarray.pydata.org/en/stable/generated/xarray.open_rasterio.html#xarray.open_rasterio
    for possible extra arguments

    Parameters
    ----------
    urlpath: str
        Path to source file. Must be a single local file of a type
        supported by rasterIO.
    chunks: int or dict
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    """
    name = 'rasterio'

    def __init__(self, urlpath, chunks, xarray_kwargs=None, metadata=None):
        self.urlpath = urlpath
        self.chunks = chunks
        self._kwargs = xarray_kwargs or {}
        self._ds = None
        super(RasterIOSource, self).__init__(metadata=metadata)

    def _open_dataset(self):
        self._ds = xr.open_rasterio(self.urlpath, chunks=self.chunks,
                                    **self._kwargs)

    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
        from .xarray_container import serialize_zarr_ds
        import msgpack
        if self._ds is None:
            self._open_dataset()

            ds2 = xr.Dataset({'raster': self._ds})
            metadata = {
                'dims': dict(ds2.dims),
                'data_vars': {k: list(ds2[k].coords)
                              for k in ds2.data_vars.keys()},
                'coords': tuple(ds2.coords.keys()),
                'internal': serialize_zarr_ds(ds2),
                'array': 'raster'
            }
            for k, v in self._ds.attrs.items():
                try:
                    msgpack.packb(v)
                    metadata[k] = v
                except TypeError:
                    pass
            self._schema = Schema(
                datashape=None,
                dtype=str(self._ds.dtype),
                shape=self._ds.shape,
                npartitions=self._ds.data.npartitions,
                extra_metadata=metadata)

        return self._schema
