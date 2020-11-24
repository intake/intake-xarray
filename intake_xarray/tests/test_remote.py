import aiohttp
import intake
import os
import pytest
import requests
import subprocess
import time
import xarray as xr
import fsspec
import dask
import numpy

PORT = 8425 # for intake-server tests
here = os.path.abspath(os.path.dirname(__file__))
cat_file = os.path.join(here, 'data', 'catalog.yaml')
DIRECTORY = os.path.join(here, 'data')


@pytest.fixture(scope='module')
def data_server():
    ''' Serves test/data folder to http://localhost:8000 '''
    pwd = os.getcwd()
    os.chdir(DIRECTORY)
    command = ['python', '-m', 'RangeHTTPServer']
    try:
        P = subprocess.Popen(command)
        timeout = 10
        while True:
            try:
                requests.get('http://localhost:8000')
                break
            except:
                time.sleep(0.1)
                timeout -= 0.1
                assert timeout > 0
        yield 'http://localhost:8000'
    finally:
        os.chdir(pwd)
        P.terminate()
        P.communicate()


def test_list_server_files(data_server):
    test_files = ['RGB.byte.tif', 'example_1.nc', 'example_2.nc', 'little_green.tif', 'little_red.tif']
    h = fsspec.filesystem("http")
    out = h.glob(data_server + '/')
    assert len(out) > 0
    assert set([data_server+'/'+x for x in test_files]).issubset(set(out))

# REMOTE GEOTIFF
def test_open_rasterio(data_server):
    url = f'{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url)
    da = source.read()
    assert isinstance(da, xr.core.dataarray.DataArray)
    assert isinstance(da.data, numpy.ndarray)


def test_open_rasterio_dask(data_server):
    url = f'{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url, chunks={})
    da = source.to_dask()
    assert isinstance(da, xr.core.dataarray.DataArray)
    assert isinstance(da.data, dask.array.core.Array)


def test_read_rasterio(data_server):
    url = f'{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url, chunks={})
    da = source.read()
    assert da.attrs['crs'] == '+init=epsg:32618'
    assert da.attrs['AREA_OR_POINT'] == 'Area'
    assert da.dtype == 'uint8'
    assert da.isel(band=2,x=300,y=500).values == 129


def test_open_rasterio_auth(data_server):
    url = f'{data_server}/RGB.byte.tif'
    auth = dict(client_kwargs={'auth': aiohttp.BasicAuth('USER', 'PASS')})
    # NOTE: if url startswith 'https' use 'https' instead of 'http' for storage_options
    source = intake.open_rasterio(url,
                                  storage_options=dict(http=auth))
    source_auth = source.storage_options['http'].get('client_kwargs').get('auth')
    assert isinstance(source_auth, aiohttp.BasicAuth)


def test_open_rasterio_simplecache(data_server):
    url = f'simplecache::{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url, chunks={})
    da = source.to_dask()
    assert isinstance(da, xr.core.dataarray.DataArray)


def test_open_rasterio_pattern(data_server):
    url = [data_server+'/'+x for x in ('little_red.tif', 'little_green.tif')]
    source = intake.open_rasterio(url,
                                  path_as_pattern='{}/little_{color}.tif',
                                  concat_dim='color',
                                  chunks={})
    da = source.to_dask()
    assert isinstance(da, xr.core.dataarray.DataArray)
    assert set(da.color.data) == set(['red', 'green'])
    assert da.shape == (2, 3, 64, 64)


# REMOTE NETCDF / HDF
def test_open_netcdf(data_server):
    url = f'{data_server}/example_1.nc'
    source = intake.open_netcdf(url)
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataset.Dataset)
    assert isinstance(ds.temp.data, numpy.ndarray)


def test_read_netcdf(data_server):
    url = f'{data_server}/example_1.nc'
    source = intake.open_netcdf(url)
    ds = source.read()
    assert ds['rh'].isel(lat=0,lon=0,time=0).values.dtype == 'float32'
    assert ds['rh'].isel(lat=0,lon=0,time=0).values == 0.5


def test_open_netcdf_dask(data_server):
    url = f'{data_server}/next_example_1.nc'
    source = intake.open_netcdf(url, chunks={},
                                xarray_kwargs=dict(engine='h5netcdf'))
    ds = source.to_dask()
    assert isinstance(ds._file_obj, xr.backends.h5netcdf_.H5NetCDFStore)
    assert isinstance(ds, xr.core.dataset.Dataset)
    assert isinstance(ds.temp.data, dask.array.core.Array)


def test_open_netcdf_simplecache(data_server):
    url = f'simplecache::{data_server}/example_1.nc'
    source = intake.open_netcdf(url, chunks={})
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataset.Dataset)
    assert isinstance(ds.temp.data, dask.array.core.Array)


# Remote catalogs with intake-server
@pytest.fixture(scope='module')
def intake_server():
    command = ['intake-server', '-p', str(PORT), cat_file]
    try:
        P = subprocess.Popen(command)
        timeout = 10
        while True:
            try:
                requests.get('http://localhost:{}'.format(PORT))
                break
            except:
                time.sleep(0.1)
                timeout -= 0.1
                assert timeout > 0
        yield 'intake://localhost:{}'.format(PORT)
    finally:
        P.terminate()
        P.communicate()


def test_remote_netcdf(intake_server):
    cat_local = intake.open_catalog(cat_file)
    cat = intake.open_catalog(intake_server)
    assert 'xarray_source' in cat
    source = cat.xarray_source()
    assert isinstance(source._ds, xr.Dataset)
    assert source._schema is None
    source._get_schema()
    assert source._schema is not None
    repr(source.to_dask())
    assert (source.to_dask().rh.data.compute() ==
            cat_local.xarray_source.to_dask().rh.data.compute()).all()
    assert (source.read() ==
            cat_local.xarray_source.read()).all()


def test_remote_tiff(intake_server):
    pytest.importorskip('rasterio')
    cat_local = intake.open_catalog(cat_file)
    cat = intake.open_catalog(intake_server)
    assert 'tiff_source' in cat
    source = cat.tiff_source()
    assert isinstance(source._ds, xr.Dataset)
    assert source._schema is None
    source._get_schema()
    assert source._schema is not None
    repr(source.to_dask())
    remote = source.to_dask().data.compute()
    local = cat_local.tiff_source.to_dask().data.compute()
    assert (remote == local).all()
    assert (source.read() ==
            cat_local.xarray_source.read()).all()
