# Tests for intake-server, local HTTP file server, local "S3" object server
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
import s3fs

here = os.path.abspath(os.path.dirname(__file__))
cat_file = os.path.join(here, 'data', 'catalog.yaml')
DIRECTORY = os.path.join(here, 'data')


@pytest.fixture(scope='module')
def data_server():
    ''' Serves test/data folder to http://localhost:8000 '''
    pwd = os.getcwd()
    os.chdir(DIRECTORY)
    command = ['python', '-m', 'RangeHTTPServer']
    success = False
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
        success = False
        yield 'http://localhost:8000'
    finally:
        os.chdir(pwd)
        P.terminate()
        out = P.communicate()
        if not success:
            print(out)


def test_http_server_files(data_server):
    test_files = ['RGB.byte.tif', 'example_1.nc', 'example_2.nc', 'little_green.tif', 'little_red.tif']
    h = fsspec.filesystem("http")
    out = h.glob(data_server + '/')
    assert len(out) > 0
    assert set([data_server+'/'+x for x in test_files]).issubset(set(out))

# REMOTE GEOTIFF
def test_http_open_rasterio(data_server):
    url = f'{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url)
    da = source.to_dask()
    assert isinstance(da, xr.core.dataarray.DataArray)


def test_http_read_rasterio(data_server):
    url = f'{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url)
    da = source.read()
    # Following line: original file CRS appears to be updated
    assert "+init" in da.attrs['crs'] or "+proj" in da.attrs['crs']
    assert da.attrs['AREA_OR_POINT'] == 'Area'
    assert da.dtype == 'uint8'
    assert da.isel(band=2,x=300,y=500).values == 129


def test_http_open_rasterio_dask(data_server):
    url = f'{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url, chunks={})
    da = source.to_dask()
    assert isinstance(da, xr.core.dataarray.DataArray)
    assert isinstance(da.data, dask.array.core.Array)


def test_http_open_rasterio_auth(data_server):
    url = f'{data_server}/RGB.byte.tif'
    auth = dict(client_kwargs={'auth': aiohttp.BasicAuth('USER', 'PASS')})
    # NOTE: if url startswith 'https' use 'https' instead of 'http' for storage_options
    source = intake.open_rasterio(url,
                                  storage_options=dict(http=auth))
    source_auth = source.storage_options['http'].get('client_kwargs').get('auth')
    assert isinstance(source_auth, aiohttp.BasicAuth)


def test_http_read_rasterio_simplecache(data_server):
    url = f'simplecache::{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url, chunks={})
    da = source.to_dask()
    assert isinstance(da, xr.core.dataarray.DataArray)


def test_http_read_rasterio_pattern(data_server):
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
def test_http_open_netcdf(data_server):
    url = f'{data_server}/example_1.nc'
    source = intake.open_netcdf(url)
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataset.Dataset)
    assert isinstance(ds.temp.data, numpy.ndarray)


def test_http_read_netcdf(data_server):
    url = f'{data_server}/example_1.nc'
    source = intake.open_netcdf(url)
    ds = source.read()
    assert ds['rh'].isel(lat=0,lon=0,time=0).values.dtype == 'float32'
    assert ds['rh'].isel(lat=0,lon=0,time=0).values == 0.5


def test_http_read_netcdf_dask(data_server):
    url = f'{data_server}/next_example_1.nc'
    source = intake.open_netcdf(url, chunks={},
                                xarray_kwargs=dict(engine='h5netcdf'))
    ds = source.to_dask()
    # assert isinstance(ds._file_obj, xr.backends.h5netcdf_.H5NetCDFStore)
    assert isinstance(ds, xr.core.dataset.Dataset)
    assert isinstance(ds.temp.data, dask.array.core.Array)


def test_http_read_netcdf_simplecache(data_server):
    url = f'simplecache::{data_server}/example_1.nc'
    source = intake.open_netcdf(
        url, chunks={},
        xarray_kwargs={"engine": "netcdf4"}
    )
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataset.Dataset)
    assert isinstance(ds.temp.data, dask.array.core.Array)


# S3
#based on: https://github.com/dask/s3fs/blob/master/s3fs/tests/test_s3fs.py
test_bucket_name = "test"
test_files = ['RGB.byte.tif', 'example_1.nc']

from s3fs.tests.test_s3fs import s3, s3_base, endpoint_uri


def test_s3_list_files(s3):
    for x in test_files:
        file_name = os.path.join(DIRECTORY,x)
        s3.put(file_name, f"{test_bucket_name}/{x}")
    s3 = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
    files = s3.ls(test_bucket_name)
    assert len(files) > 0
    assert set([test_bucket_name+'/'+x for x in test_files]).issubset(set(files))


def test_s3_read_rasterio(s3):
    # Lots of GDAL Environment variables needed for this to work !
    # https://gdal.org/user/virtual_file_systems.html#vsis3-aws-s3-files
    for x in test_files:
        file_name = os.path.join(DIRECTORY,x)
        s3.put(file_name, f"{test_bucket_name}/{x}")
    os.environ['AWS_NO_SIGN_REQUEST']='YES'
    os.environ['AWS_S3_ENDPOINT'] = endpoint_uri.lstrip('http://')
    os.environ['AWS_VIRTUAL_HOSTING']= 'FALSE'
    os.environ['AWS_HTTPS']= 'NO'
    os.environ['GDAL_DISABLE_READDIR_ON_OPEN']='EMPTY_DIR'
    os.environ['CPL_CURL_VERBOSE']='YES'
    url = f's3://{test_bucket_name}/RGB.byte.tif'
    source = intake.open_rasterio(url)
    da = source.read()
    # Following line: original file CRS appears to be updated
    assert "+init" in da.attrs['crs'] or "+proj" in da.attrs['crs']
    assert da.attrs['AREA_OR_POINT'] == 'Area'
    assert da.dtype == 'uint8'
    assert da.isel(band=2,x=300,y=500).values == 129


def test_s3_read_netcdf(s3):
    for x in test_files:
        file_name = os.path.join(DIRECTORY,x)
        s3.put(file_name, f"{test_bucket_name}/{x}")
    url = f's3://{test_bucket_name}/example_1.nc'
    s3options = dict(client_kwargs={"endpoint_url": endpoint_uri})
    source = intake.open_netcdf(url,
                                storage_options=s3options)
    ds = source.read()
    assert ds['rh'].isel(lat=0,lon=0,time=0).values.dtype == 'float32'
    assert ds['rh'].isel(lat=0,lon=0,time=0).values == 0.5


# Remote catalogs with intake-server
@pytest.fixture(scope='module')
def intake_server():
    PORT = 8002
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


def test_intake_server_netcdf(intake_server):
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


def test_intake_server_tiff(intake_server):
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
