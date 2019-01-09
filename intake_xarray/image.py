import xarray as xr
from .base import XarraySource


def add_leading_dimension(x):
    return x[None, ...]


def dask_imread(filename, imread=None, preprocess=None):
    """ Read a stack of images into a dask array
    Parameters
    ----------
    filename: string, iter
        A globstring like 'myfile.*.png', a list of files, or anything
        that can go into imread.
    imread: function (optional)
        Optionally provide custom imread function.
        Function should expect a filename and produce a numpy array.
        Defaults to ``skimage.io.imread``.
    preprocess: function (optional)
        Optionally provide custom function to preprocess the image.
        Function should expect a numpy array for a single image.
    Examples
    --------
    >>> from dask.array.image import imread
    >>> im = imread('2015-*-*.png')  # doctest: +SKIP
    >>> im.shape  # doctest: +SKIP
    (365, 1000, 1000, 3)
    Returns
    -------
    Dask array of all images stacked along the first dimension.  All images
    will be treated as individual chunks
    """
    import os
    from glob import glob

    try:
        from skimage.io import imread as sk_imread
    except (AttributeError, ImportError):
        pass

    from dask.array import Array
    from dask.base import tokenize

    imread = imread or sk_imread
    name = None

    if isinstance(filename, list):
        filenames = filename
    elif '*' in filename:
        filenames = sorted(glob(filename))
    elif os.path.isfile(filename):
        filenames = [filename]
    else:
        filenames = [filename]
        name = 'imread-%s' % tokenize(filenames)

    if not filenames:
        raise ValueError("No files found under name %s" % filename)

    name = name or 'imread-%s' % tokenize(filenames, map(os.path.getmtime, filenames))

    sample = imread(filenames[0])
    if preprocess:
        sample = preprocess(sample)

    keys = [(name, i) + (0,) * len(sample.shape) for i in range(len(filenames))]
    if preprocess:
        values = [(add_leading_dimension, (preprocess, (imread, fn)))
                  for fn in filenames]
    else:
        values = [(add_leading_dimension, (imread, fn))
                  for fn in filenames]
    dsk = dict(zip(keys, values))

    chunks = ((1, ) * len(filenames), ) + tuple((d, ) for d in sample.shape)

    return Array(dsk, name, chunks, sample.dtype)


class ImageSource(XarraySource):
    """Open a xarray dataset from image files.

    This creates an xarray.DataArray or an xarray.Dataset.

    See http://scikit-image.org/docs/dev/api/skimage.io.html#skimage.io.imread
    for the file formats supported, and
     http://docs.dask.org/en/latest/array-api.html#dask.array.image.imread
    for possible extra arguments.

    NOTE: Although ``skimage.io.imread`` is used by default, any reader function which
    accepts a filename and outputs a numpy array can be used instead.

    Parameters
    ----------
    urlpath : str or iterable, location of data
        May be a local path, or remote path if including a protocol specifier
        such as ``'s3://'``. May include glob wildcards or format pattern strings.
        Must be a format supported by ``skimage.io.imread`` or user-supplied ``imread``
        Some examples:
            - ``{{ CATALOG_DIR }}data/RGB.tif``
            - ``s3://data/*.jpeg``
            - ``https://example.com/image.png`
            - ``s3://data/Images/{{ landuse }}/{{ landuse }}{{ '%02d' % id }}.tif``
    xarray_kwargs : dict, optional
        Any further arguments to pass to reader function. Of particular
        interest is the ``imread`` option.
    """
    name = 'xarray_image'
    __doc__ += XarraySource.__inheritted_parameters_doc__

    def __init__(self, urlpath, chunks=None, **kwargs):
        super(ImageSource, self).__init__(urlpath, chunks, **kwargs)

    def reader(self, filename, chunks, **kwargs):
        import numpy as np

        dask_array = dask_imread(filename, **kwargs)[0]

        ny, nx = dask_array.shape[:2]
        coords = {'y': np.arange(ny),
                  'x': np.arange(nx)}
        dims = ('y', 'x')

        if len(dask_array.shape) == 3:
            nband = dask_array.shape[2]
            coords['band'] = np.arange(nband)
            dims += ('band',)

        return xr.DataArray(dask_array, coords=coords, dims=dims).chunk(chunks=chunks)

    def multireader(self, filename, chunks, concat_dim, **kwargs):
        import numpy as np

        dask_array = dask_imread(filename, **kwargs)

        ny, nx = dask_array.shape[1:3]
        coords = {'y': np.arange(ny),
                  'x': np.arange(nx)}
        dims = (concat_dim, 'y', 'x')

        if len(dask_array.shape) == 4:
            nband = dask_array.shape[3]
            coords['band'] = np.arange(nband)
            dims += ('band',)

        return xr.DataArray(dask_array, coords=coords, dims=dims).chunk(chunks=chunks)
