# -*- coding: utf-8 -*-
import os
from unittest.mock import patch
import tempfile

import numpy as np
import pytest
import xarray as xr

import intake

here = os.path.dirname(__file__).rstrip('/')


@pytest.mark.parametrize('source', ['netcdf', 'zarr'])
def test_read(source, netcdf_source, zarr_source, dataset):
    source = {'netcdf': netcdf_source, 'zarr': zarr_source}[source]

    ds = source.read_chunked()
    assert ds.temp.chunks

    ds = source.read()
    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)


def test_read_list_of_netcdf_files_with_combine_nested():
    from intake_xarray.netcdf import NetCDFSource
    source = NetCDFSource([
        os.path.join(here, 'data', 'example_1.nc'),
        os.path.join(here, 'data', 'example_2.nc'),
    ],
        combine='nested',
        concat_dim='concat_dim'
    )
    d = source.to_dask()
    assert d.dims == {'lat': 5, 'lon': 10, 'level': 4, 'time': 1,
                      'concat_dim': 2}


def test_read_glob_pattern_of_netcdf_files():
    """If xarray is old, prompt user to update to use pattern"""
    from intake_xarray.netcdf import NetCDFSource
    source = NetCDFSource(os.path.join(here, 'data', 'example_{num: d}.nc'),
                          concat_dim='num', combine='nested')
    d = source.to_dask()
    print(d.dims)
    assert d.dims == {'lat': 5, 'lon': 10, 'level': 4, 'time': 1,
                      'num': 2}
    assert (d.num.data == np.array([1, 2])).all()


@pytest.mark.parametrize('source', ['netcdf', 'zarr'])
def test_to_dask(source, netcdf_source, zarr_source, dataset):
    source = {'netcdf': netcdf_source, 'zarr': zarr_source}[source]
    ds = source.to_dask()

    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)


def test_grib_dask():
    pytest.importorskip('Nio')
    import dask.array as da
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    x = cat.grib.to_dask()
    assert len(x.fileno) == 2
    assert isinstance(x.APCP_P8_L1_GLL0_acc6h.data, da.Array)
    values = x.APCP_P8_L1_GLL0_acc6h.data.compute()
    x2 = cat.grib.read()
    assert (values == x2.APCP_P8_L1_GLL0_acc6h.values).all()


def test_rasterio():
    import dask.array as da
    pytest.importorskip('rasterio')
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    s = cat.tiff_source
    x = s.to_dask()
    assert isinstance(x.band_data.data, da.Array)
    x = s.read()
    assert x.band_data.shape == (3, 718, 791)


def test_rasterio_glob():
    import dask.array as da
    pytest.importorskip('rasterio')
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    s = cat.tiff_glob_source
    x = s.to_dask()
    assert isinstance(x.band_data.data, da.Array)
    x = s.read()
    assert x.band_data.shape == (3, 718, 791)


def test_rasterio_empty_glob():
    pytest.importorskip('rasterio')
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    s = cat.empty_glob
    with pytest.raises(Exception):
        s.read()


def test_read_pattern_concat_on_new_dim():
    pytest.importorskip('rasterio')
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    colors = cat.pattern_tiff_source_concat_on_new_dim

    da = colors.read().band_data
    assert da.shape == (2, 3, 64, 64)
    assert len(da.new_dim) == 2
    assert set(da.new_dim.data) == set(['red', 'green'])
    assert da[da.new_dim == 'red'].shape == (1, 3, 64, 64)

    rgb = {'red': [204, 17, 17], 'green': [17, 204, 17]}
    for color, values in rgb.items():
        for i, v in enumerate(values):
            assert (da[da.new_dim == color][0].sel(band=i+1).values == v).all()


def test_read_pattern_field_as_band():
    pytest.importorskip('rasterio')
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    colors = cat.pattern_tiff_source_path_pattern_field_as_band

    da = colors.read().band_data
    assert da.shape == (2, 3, 64, 64)
    assert set(da.color.data) == set(['red', 'green'])
    assert da[da.color == 'red'].squeeze().shape == (3, 64, 64)

    rgb = {'red': [204, 17, 17], 'green': [17, 204, 17]}
    for color, values in rgb.items():
        for i, v in enumerate(values):
            assert (da[da.color == color][0][i].values == v).all()


def test_read_pattern_path_as_pattern_as_str_with_list_of_urlpaths():
    pytest.importorskip('rasterio')
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    colors = cat.pattern_tiff_source_path_pattern_as_str

    da = colors.read().band_data
    assert da.shape == (2, 3, 64, 64)
    assert len(da.color) == 2
    assert set(da.color.data) == set(['red', 'green'])

    assert da.sel(color='red').shape == (3, 64, 64)

    rgb = {'red': [204, 17, 17], 'green': [17, 204, 17]}
    for color, values in rgb.items():
        for i, v in enumerate(values):
            assert (da.sel(color=color).sel(band=i+1).values == v).all()


def test_read_image():
    pytest.importorskip('skimage')
    from intake_xarray.image import ImageSource
    im = ImageSource(os.path.join(here, 'data', 'little_red.tif'))
    da = im.read()
    assert da.shape == (64, 64, 3)


def test_read_images():
    pytest.importorskip('skimage')
    from intake_xarray.image import ImageSource
    im = ImageSource(os.path.join(here, 'data', 'little_*.tif'))
    da = im.read()
    assert da.shape == (2, 64, 64, 3)
    assert da.dims == ('concat_dim', 'y', 'x', 'channel')


def test_read_images_with_pattern():
    pytest.importorskip('skimage')
    from intake_xarray.image import ImageSource
    path = os.path.join(here, 'data', 'little_{color}.tif')
    im = ImageSource(path, concat_dim='color')
    da = im.read()
    assert da.shape == (2, 64, 64, 3)
    assert len(da.color) == 2
    assert set(da.color.data) == set(['red', 'green'])


def test_read_images_with_multiple_concat_dims_with_pattern():
    pytest.importorskip('skimage')
    from intake_xarray.image import ImageSource
    path = os.path.join(here, 'data', '{size}_{color}.tif')
    im = ImageSource(path, concat_dim=['size', 'color'])
    ds = im.read()
    assert ds.sel(color='red', size='little').shape == (64, 64, 3)


def test_read_jpg_image():
    pytest.importorskip('skimage')
    from intake_xarray.image import ImageSource
    im = ImageSource(os.path.join(here, 'data', 'dog.jpg'))
    da = im.read()
    assert da.shape == (192, 192)


@pytest.mark.parametrize("engine", ["pydap", "netcdf4"])
def test_read_opendap_no_auth(engine):
    pytest.importorskip("pydap")
    cat = intake.open_catalog(os.path.join(here, "data", "catalog.yaml"))
    source = cat["opendap_source_{}".format(engine)]
    x = source.read()
    assert x.TIME.shape == (12,)


@pytest.mark.parametrize("auth", ["esgf", "urs"])
def test_read_opendap_with_auth(auth):
    pytest.importorskip("pydap")
    from intake_xarray.opendap import OpenDapSource

    os.environ["DAP_USER"] = "username"
    os.environ["DAP_PASSWORD"] = "password"
    urlpath = "http://test.opendap.org/opendap/hyrax/data/nc/123.nc"

    with patch(
        f"pydap.cas.{auth}.setup_session", return_value=None
    ) as mock_setup_session:
        source = OpenDapSource(urlpath=urlpath, chunks={}, auth=auth, engine="pydap")
        source.discover()
        mock_setup_session.assert_called_once_with(
            os.environ["DAP_USER"], os.environ["DAP_PASSWORD"], check_url=urlpath
        )


def test_read_opendap_mfdataset_with_engine():
    pytest.importorskip("pydap")
    from intake_xarray.opendap import OpenDapSource
    urls = [
        'http://example.com/opendap/fake1.nc',
        'http://example.com/opendap/fake2.nc',
    ]
    with patch('xarray.open_mfdataset') as open_mfdataset_mock:
        open_mfdataset_mock.return_value = 'dataset'
        source = OpenDapSource(urlpath=urls, chunks={}, auth=None, engine='fake-engine')
        retval = source.read()
        assert open_mfdataset_mock.called_with(urls, chunks={}, engine='fake-engine')
    assert retval == 'dataset'


@pytest.mark.parametrize("auth", ["esgf", "urs"])
def test_read_opendap_with_auth_netcdf4(auth):
    from intake_xarray.opendap import OpenDapSource

    os.environ["DAP_USER"] = "username"
    os.environ["DAP_PASSWORD"] = "password"
    urlpath = "http://test.opendap.org/opendap/hyrax/data/nc/123.nc"

    with patch(
        f"pydap.cas.{auth}.setup_session", return_value=1
    ) as mock_setup_session:
        source = OpenDapSource(urlpath=urlpath, chunks={}, auth=auth, engine="pydap")
        with pytest.raises(Exception):
            source.discover()


def test_read_opendap_invalid_auth():
    pytest.importorskip("pydap")
    from intake_xarray.opendap import OpenDapSource

    source = OpenDapSource(urlpath="https://test.url", chunks={}, auth="abcd", engine="pydap")
    with pytest.raises(Exception):
        source.discover()


def test_read_opendap_invalid_engine():
    from intake_xarray.opendap import OpenDapSource

    source = OpenDapSource(urlpath="https://test.url", chunks={}, auth=None, engine="abcd")
    with pytest.raises(ValueError):
        source.discover()


def test_cached_list_netcdf():
    tempd = str(tempfile.mkdtemp())
    from intake_xarray.netcdf import NetCDFSource
    source = NetCDFSource([
        'filecache://' + os.path.join(here, 'data', 'example_1.nc'),
        'filecache://' + os.path.join(here, 'data', 'example_2.nc'),
        ],
        combine='nested',
        concat_dim='concat_dim',
        storage_options={'cache_storage': tempd, 'target_protocol': 'file'},
        xarray_kwargs={"engine": "scipy"},
        open_local=True,
    )
    d = source.read()
    assert d.dims == {'lat': 5, 'lon': 10, 'level': 4, 'time': 1,
                      'concat_dim': 2}
    assert os.listdir(tempd)
