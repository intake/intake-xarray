from .base import XarraySource


class RasterIOSource(XarraySource):
    """Open a xarray object (dataarray or dataset) via RasterIO.

    This creates an xarray.DataArray or an xarray.Dataset.

    See https://rasterio.readthedocs.io/en/latest/ for the file formats
    supported, particularly GeoTIFF, and
    http://xarray.pydata.org/en/stable/generated/xarray.open_rasterio.html#xarray.open_rasterio
    for possible extra arguments

    Parameters
    ----------
    urlpath: str or iterable, location of data
        May be a local path, or remote path if including a protocol specifier
        such as ``'s3://'``. May include glob wildcards or format pattern strings.
        Must be a format supported by rasterIO (normally GeoTiff).
        Some examples:
            - ``{{ CATALOG_DIR }}data/RGB.tif``
            - ``s3://data/*.tif``
            - ``s3://data/landsat8_band{band}.tif``
            - ``s3://data/{location}/landsat8_band{band}.tif``
            - ``{{ CATALOG_DIR }}data/landsat8_{start_date:%Y%m%d}_band{band}.tif``
    xarray_kwargs : dict, optional
        Any further arguments to pass to ``xr.open_rasterio``.
    """
    name = 'rasterio'
    __doc__ += XarraySource.__inheritted_parameters_doc__

    def __init__(self, urlpath, chunks=None, **kwargs):
        from xarray import open_rasterio

        super(RasterIOSource, self).__init__(urlpath, chunks, **kwargs)
        self.reader = open_rasterio
        self.multireader = None
