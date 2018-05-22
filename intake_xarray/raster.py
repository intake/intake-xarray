import xarray as xr
from intake.source import base
from . import DataSourceMixin


class RasterIOSource(DataSourceMixin, base.DataSource):
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

    def __init__(self, urlpath, chunks, xarray_kwargs=None, metadata=None):
        self.urlpath = urlpath
        self.chunks = chunks
        self._kwargs = xarray_kwargs or {}
        self._ds = None
        super(RasterIOSource, self).__init__(
            container='xarray', metadata=metadata)

    def _open_dataset(self):
        self._ds = xr.open_rasterio(self.urlpath, chunks=self.chunks,
                                    **self._kwargs)

    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
        if self._ds is None:
            self._open_dataset()

        metadata = {
            'dims': self._ds.dims,
            'coords': tuple(self._ds.coords.keys())
        }
        metadata.update(self._ds.attrs)
        return base.Schema(
            datashape=None,
            dtype=self._ds,
            shape=self._ds.shape,
            npartitions=None,
            extra_metadata=metadata)
