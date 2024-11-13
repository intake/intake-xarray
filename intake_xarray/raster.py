from intake import readers
from intake.readers.utils import pattern_to_glob
from intake.source.utils import reverse_formats

from intake_xarray.base import IntakeXarraySourceAdapter


class RasterIOSource(IntakeXarraySourceAdapter):
    """Open a xarray dataset via RasterIO.

    This creates an xarray.array, not a dataset (i.e., there is exactly one
    variable).

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
    chunks: None or int or dict, optional
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays. default `None` loads numpy arrays.
    path_as_pattern: bool or str, optional
        Whether to treat the path as a pattern (ie. ``data_{field}.tif``)
        and create new coodinates in the output corresponding to pattern
        fields. If str, is treated as pattern to match on. Default is True.
    """
    name = 'rasterio'
    container = "xarray"

    def __init__(self, urlpath,
                 xarray_kwargs=None, metadata=None, path_as_pattern=True,
                 storage_options=None, **kwargs):
        data = readers.datatypes.TIFF(urlpath, storage_options=storage_options)
        if (path_as_pattern is True and "{" in urlpath) or isinstance(path_as_pattern, str):
            reader = readers.XArrayPatternReader(data, **(xarray_kwargs or {}), metadata=metadata, engine="rasterio",
                                                 pattern=path_as_pattern, **kwargs)
        else:
            reader = readers.XArrayDatasetReader(data, **(xarray_kwargs or {}), metadata=metadata, engine="rasterio", **kwargs)
        self.reader = reader
