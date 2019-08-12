# -*- coding: utf-8 -*-
import os
import numpy as np
import pytest

from intake_xarray.image import _coerce_shape, ImageSource

here = os.path.dirname(__file__)


@pytest.mark.parametrize('im', [
    [[1, 2],
     [3, 4]],
    [[1, 2, 7],
     [3, 4, 6]],
    [[1, 2, 7],
     [3, 4, 6],
     [5, 6, 8]],
    [[1, 2],
     [3, 4],
     [5, 6],
     [7, 8]],
])
def test_coerce_shape_2d_trim_only(im):
    shape = (2, 2)
    array = np.array(im)
    expected = np.array([[1, 2],
                         [3, 4]])
    actual = _coerce_shape(array, shape)
    assert (expected == actual).all()


def test_coerce_shape_2d_pad_only():
    shape = (3, 4)
    array = np.array([[1, 2],
                      [3, 4]])
    expected = np.array([[1, 2, 0, 0],
                         [3, 4, 0, 0],
                         [0, 0, 0, 0]])
    actual = _coerce_shape(array, shape)
    assert (expected == actual).all()


def test_coerce_shape_2d_pad_nrows_and_trim_ncols():
    shape = (4, 2)
    array = np.array([[1, 2, 7],
                      [3, 4, 6]])
    expected = np.array([[1, 2],
                         [3, 4],
                         [0, 0],
                         [0, 0]])
    actual = _coerce_shape(array, shape)
    assert (expected == actual).all()


def test_coerce_shape_2d_pad_ncols_and_trim_nrows():
    shape = (2, 4)
    array = np.array([[1, 2],
                      [3, 4],
                      [5, 6],
                      [7, 8]])
    expected = np.array([[1, 2, 0, 0],
                         [3, 4, 0, 0]])
    actual = _coerce_shape(array, shape)
    assert (expected == actual).all()


def test_coerce_shape_3d_no_change():
    shape = (3, 3)
    array = np.arange(3**3).reshape(3, 3, 3)
    actual = _coerce_shape(array, shape)
    assert (array == actual).all()


def test_coerce_shape_3d_pad_nrows_and_trim_ncols():
    shape = (5, 2)
    array = np.arange(2*4*3).reshape(2, 4, 3)
    expected = np.array([[[0,  1,  2],
                          [3,  4,  5]],

                         [[12, 13, 14],
                          [15, 16, 17]],

                         [[0,  0,  0],
                          [0,  0,  0]],

                         [[0,  0,  0],
                          [0,  0,  0]],

                         [[0,  0,  0],
                          [0,  0,  0]]])
    actual = _coerce_shape(array, shape)
    assert (expected == actual).all()


def test_coerce_shape_3d_pad_ncols_and_trim_nrows():
    shape = (2, 5)
    array = np.arange(3*2*4).reshape(3, 2, 4)
    expected = np.array([[[0,  1,  2,  3],
                          [4,  5,  6,  7],
                          [0,  0,  0,  0],
                          [0,  0,  0,  0],
                          [0,  0,  0,  0]],

                         [[8,  9, 10, 11],
                          [12, 13, 14, 15],
                          [0,  0,  0,  0],
                          [0,  0,  0,  0],
                          [0,  0,  0,  0]]])
    actual = _coerce_shape(array, shape)
    assert (expected == actual).all()


def test_coerce_shape_raises_error_if_shape_not_len_2():
    shape = (2, 3, 3)
    array = np.arange(3**3).reshape(3, 3, 3)
    with pytest.raises(ValueError,
                       match='coerce_shape must be an iterable of len 2'):
        _coerce_shape(array, shape)


def test_coerce_shape_array_non_int():
    shape = (2, 3)
    array = np.random.random((3, 2))
    expected = np.append(array[:2, :], [[0], [0]], axis=1)
    actual = _coerce_shape(array, shape)
    assert (expected == actual).all()
    assert expected.dtype == np.float


def test_read_image():
    pytest.importorskip('skimage')
    urlpath = os.path.join(here, 'data', 'images', 'beach57.tif')
    source = ImageSource(urlpath=urlpath)
    array = source.read()
    assert array.shape == (256, 252, 3)
    assert array.dtype == np.uint8


def test_read_images_as_glob_without_coerce_raises_error():
    pytest.importorskip('skimage')
    urlpath = os.path.join(here, 'data', 'images', '*')
    source = ImageSource(urlpath=urlpath)
    with pytest.raises(ValueError,
                       match='could not broadcast input array'):
        source.read()


def test_read_images_as_glob_with_coerce():
    pytest.importorskip('skimage')
    urlpath = os.path.join(here, 'data', 'images', '*')
    source = ImageSource(urlpath=urlpath, coerce_shape=(256, 256))
    array = source.read()
    assert array.shape == (3, 256, 256, 3)
