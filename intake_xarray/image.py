import fsspec

from intake.source.utils import reverse_formats
from intake import readers

from intake_xarray.base import IntakeXarraySourceAdapter


def _coerce_shape(array, shape):
    """ Trim or pad array to match desired shape"""
    import numpy as np

    if len(shape) != 2:
        raise ValueError('coerce_shape must be an iterable of len 2')

    target_shape = tuple(shape)
    actual_shape = array.shape
    ndims = len(actual_shape)

    if actual_shape[:2] == target_shape:
        # no trimming or padding needed
        return array

    # do any necessary trimming first
    for i, (a, t) in enumerate(zip(actual_shape[:2], target_shape)):
        if a > t:
            if i == 0:
                if ndims == 2:
                    array = array[:t, :]
                else:
                    array = array[:t, :, :]
            else:
                if ndims == 2:
                    array = array[:, :t]
                else:
                    array = array[:, :t, :]

    if array.shape[:2] == target_shape:
        # only needed trimming
        return array

    # create array of zeros and fill with trimmed value array
    if ndims == 2:
        new_array = np.zeros(target_shape, dtype=array.dtype)
        new_array[:array.shape[0], :array.shape[1]] = array
    else:
        new_array = np.zeros((target_shape[0],
                              target_shape[1],
                              actual_shape[2]), dtype=array.dtype)
        new_array[:array.shape[0], :array.shape[1], :] = array

    return new_array


def _add_leading_dimension(x):
    """Add a new dimension to an array-like"""
    return x[None, ...]


def _dask_imread(files, imread=None, preprocess=None, coerce_shape=None):
    """ Read a stack of images into a dask array """
    from dask.array import Array
    from dask.base import tokenize
    from functools import partial

    if not imread:
        from skimage.io import imread

    def _imread(open_file):
        with open_file as f:
            return imread(f)

    filenames = [f.path for f in files]

    name = 'imread-%s' % tokenize(filenames)

    if coerce_shape is not None:
        reshape = partial(_coerce_shape, shape=coerce_shape)

    with files[0] as f:
        sample = imread(f)
    if coerce_shape is not None:
        sample = reshape(sample)
    if preprocess:
        sample = preprocess(sample)

    keys = [(name, i) + (0,) * len(sample.shape)
            for i in range(len(files))]

    if coerce_shape is not None:
        if preprocess:
            values = [(_add_leading_dimension,
                       (preprocess,
                        (reshape,
                         (_imread, f))))
                      for f in files]
        else:
            values = [(_add_leading_dimension,
                       (reshape,
                        (_imread, f)))
                      for f in files]
    elif preprocess:
        values = [(_add_leading_dimension,
                   (preprocess,
                    (_imread, f)))
                  for f in files]
    else:
        values = [(_add_leading_dimension,
                   (_imread, f))
                  for f in files]
    dsk = dict(zip(keys, values))

    chunks = ((1, ) * len(files), ) + tuple((d, ) for d in sample.shape)

    return Array(dsk, name, chunks, sample.dtype)


def _dask_exifread(files, exif_tags):
    """Construct a dask Array to read each tag in `exif_tags` (list of
    str) from the EXIF data of the images in `files`
    """
    from copy import copy
    from numpy import array
    from dask.array import Array
    from dask.base import tokenize
    from exifread import process_file as read_exif

    def _read_exif(open_file):
        # Take a fresh copy of open_file, to work around occasional
        # 'I/O operation on closed file' and similar errors when
        # open_file is also opened elsewhere
        with copy(open_file) as f:
            return read_exif(f)

    if not isinstance(exif_tags, list):
        sample = _read_exif(files[0])
        exif_tags = sample.keys()

    ntags = len(exif_tags)

    def extract_tags(d):
        return array([d.get(tag) for tag in exif_tags])

    filenames = [f.path for f in files]
    name = 'exifread-%s' % tokenize(filenames)

    keys = [(name, i, 0) for i in range(len(files))]
    values = [(_add_leading_dimension,
               (extract_tags,
                (_read_exif, f)))
              for f in files]

    dsk = dict(zip(keys, values))

    chunks = ((1,) * len(files), (ntags,))

    exif_data = Array(dsk, name, chunks, object)

    return {'EXIF ' + tag: exif_data[:,i] for i, tag in enumerate(exif_tags)}


def multireader(files, chunks, concat_dim, exif_tags, **kwargs):
    """Read a stack of images into a dask xarray object

    NOTE: copied from dask.array.image.imread but altering the input to accept
    a list of file objects.

    Parameters
    ----------
    files : iter
        List of file objects where each file contains data with the same
        shape. If this is not the case, use preprocess to coerce data into
        a shape
    chunks : int or dict
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    concat_dim : str or iterable
        Dimension over which to concatenate. If iterable, all fields must be
        part of the the pattern.
    imread : function (optional)
        Optionally provide custom imread function.
        Function should expect a file object and produce a numpy array.
        Defaults to ``skimage.io.imread``.
    preprocess : function (optional)
        Optionally provide custom function to preprocess the image.
        Function should expect a numpy array for a single image.
    coerce_shape : iterable of len 2 (optional)
        Optionally coerce the shape of the height and width of the image
        by setting `coerce_shape` to desired shape.
    exif_tags : boolean or list of str (optional)
        Controls whether exif tags are extracted from the images. If a
        list, the elements are treated as the particular tags to
        extract from each image. For any other truthy value, all tags
        that were able to be extracted from a sample image are used.
        When tags are extracted, an xarray Dataset is returned, with
        each exif tag in a corresponding data variable of the Dataset,
        (of type `Optional[exifread.classes.IfdTag]`), and the image
        data in a data variable 'raster'.

    Returns
    -------
    A Dask xarray.DataArray or xarray.Dataset, of all images stacked
    along the first dimension, and (optionally) the value of any
    requested EXIF tags.  All images will be treated as individual
    chunks unless chunks kwarg is specified.
    """
    import numpy as np
    from xarray import DataArray, Dataset

    dask_array = _dask_imread(files, **kwargs)

    ny, nx = dask_array.shape[1:3]
    coords = {'y': np.arange(ny),
              'x': np.arange(nx)}
    if isinstance(concat_dim, list):
        dims = ('dim_0',)
    else:
        dims = (concat_dim,)
        coords = {concat_dim: np.arange(dask_array.shape[0]),
                  **coords}

    raster_dims = dims + ('y', 'x')
    if len(dask_array.shape) == 4:
        nchannel = dask_array.shape[3]
        coords['channel'] = np.arange(nchannel)
        raster_dims += ('channel',)

    if exif_tags:
        exif_dict = _dask_exifread(files, exif_tags)
        exif_dict_ds = {tag: (dims, arr) for tag, arr in exif_dict.items()}
        return Dataset(
            {
                'raster': (raster_dims, dask_array),
                **exif_dict_ds,
            },
            coords=coords,
        ).chunk(chunks=chunks)
    else:
        return DataArray(
            dask_array, coords=coords, dims=raster_dims
        ).chunk(chunks=chunks)


class ImageReader(readers.BaseReader):
    """Open a xarray dataset from image files.

    This creates an xarray.DataArray or an xarray.Dataset.
    See http://scikit-image.org/docs/dev/api/skimage.io.html#skimage.io.imread
    for the file formats supported.

    Parameters
    ----------
    urlpath : str or iterable, location of data
        May be a local path, or remote path if including a protocol specifier
        such as ``'s3://'``. May include glob wildcards or format pattern
        strings. Must be a format supported by ``skimage.io.imread`` or
        user-supplied ``imread``. Some examples:
            - ``{{ CATALOG_DIR }}/data/RGB.tif``
            - ``s3://data/*.jpeg``
            - ``https://example.com/image.png``
            - ``s3://data/Images/{{ landuse }}/{{ '%02d' % id }}.tif``
    chunks : int or dict
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    path_as_pattern : bool or str, optional
        Whether to treat the path as a pattern (ie. ``data_{field}.tif``)
        and create new coodinates in the output corresponding to pattern
        fields. If str, is treated as pattern to match on. Default is True.
    concat_dim : str or iterable
        Dimension over which to concatenate. If iterable, all fields must be
        part of the the pattern.
    preprocess : function (optional)
        Optionally provide custom function to preprocess the image.
        Function should expect a numpy array for a single image and return
        a numpy array.
    coerce_shape : iterable of len 2 (optional)
        Optionally coerce the shape of the height and width of the image
        by setting `coerce_shape` to desired shape.
    exif_tags : boolean or list of str (optional)
        Controls whether exif tags are extracted from the images. If a
        list, the elements are treated as the particular tags to
        extract from each image. For any other truthy value, all tags
        that were able to be extracted from a sample image are used.
        When tags are extracted, an xarray Dataset is returned, with
        each exif tag in a corresponding data variable of the Dataset,
        (of type `Optional[exifread.classes.IfdTag]`), and the image
        data in a data variable 'raster'.

    """
    output_instance = "xarray:Dataset"


    def _read(self, urlpath, chunks=None, concat_dim='concat_dim',
              metadata=None, path_as_pattern=None,
              storage_options=None, exif_tags=None, **kwargs):
        """
        This function is called when the data source refers to more
        than one file either as a list or a glob. It sets up the
        dask graph for opening the files.

        Parameters
        ----------
        files : iter
            List of file objects
        """
        import pandas as pd
        from xarray import DataArray
        path_as_pattern = path_as_pattern or (path_as_pattern is None and "{" in urlpath)

        if path_as_pattern:
            from intake.readers.utils import pattern_to_glob

            url = pattern_to_glob(urlpath)
            __, _, paths = fsspec.get_fs_token_paths(url, **(storage_options or {}))
            field_values = reverse_formats(urlpath, paths)
            paths = paths
        else:
            paths = urlpath

        files = fsspec.open_files(paths, **(storage_options or {}))

        out = multireader(
            files, chunks, concat_dim, exif_tags, **kwargs
        )
        if isinstance(out, DataArray) and len(files) == 1 and isinstance(urlpath, str) and "*" not in urlpath:
            out = out[0]
        if not path_as_pattern:
            return out

        coords = {}
        filenames = [f.path for f in files]

        if isinstance(concat_dim, list):
            if not set(field_values.keys()).issuperset(set(concat_dim)):
                raise KeyError('All concat_dims should be in pattern.')
            index = pd.MultiIndex.from_tuples(
                zip(*(field_values[dim] for dim in concat_dim)),
                names=concat_dim)
            coords = {
                k: DataArray(v, dims=('dim_0'))
                for k, v in field_values.items() if k not in concat_dim
            }
            out = (out.assign_coords(dim_0=index, **coords)  # use the index
                      .unstack().chunk(chunks))  # unstack along new index
            return out.transpose(*concat_dim,  # reorder dims
                                 *filter(lambda x: x not in concat_dim,
                                         out.dims))
        else:
            coords = {
                k: DataArray(v, dims=concat_dim)
                for k, v in field_values.items()
            }
            return out.assign_coords(**coords).chunk(chunks).unify_chunks()


class ImageSource(IntakeXarraySourceAdapter):
    name = 'xarray_image'
    container = "xarray"

    def __init__(self, *ar, **kw):
        self.reader = ImageReader(*ar, **kw)
