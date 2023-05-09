# Tests that read public data over the internet
import intake
import pytest
import xarray as xr


# RasterIOSource
@pytest.mark.xfail
def test_open_rasterio_http():
    prefix = 'https://landsat-pds.s3.us-west-2.amazonaws.com/L8/139/045'
    image = 'LC81390452014295LGN00/LC81390452014295LGN00_B1.TIF'
    url = f'{prefix}/{image}'
    source = intake.open_rasterio(url,
                                  chunks=dict(band=1))
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataarray.DataArray)


def test_open_rasterio_s3():
    bucket = 's3://landsat-pds'
    key = 'L8/139/045/LC81390452014295LGN00/LC81390452014295LGN00_B1.TIF'
    url = f'{bucket}/{key}'
    source = intake.open_rasterio(url,
                                  chunks=dict(band=1),
                                  storage_options = dict(anon=True))
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataarray.DataArray)


# NETCDFSource
def test_open_netcdf_gs():
    bucket = 'gs://ldeo-glaciology'
    key = 'bedmachine/BedMachineAntarctica_2019-11-05_v01.nc'
    url = f'{bucket}/{key}'
    source = intake.open_netcdf(url,
                                chunks=3000,
                                xarray_kwargs=dict(engine='h5netcdf'),
                                )
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataarray.Dataset)


@pytest.mark.xfail
def test_open_netcdf_s3():
    bucket = 's3://its-live-data.jpl.nasa.gov'
    key = 'icesat2/alt06/rel003/ATL06_20181230162257_00340206_003_01.h5'
    url = f'{bucket}/{key}'
    source = intake.open_netcdf(url,
                                xarray_kwargs=dict(group='gt1l/land_ice_segments', engine='h5netcdf'),
                                storage_options=dict(anon=True),
                                )
    ds = source.to_dask()
    assert isinstance(ds._file_obj, xr.backends.h5netcdf_.H5NetCDFStore)
    assert isinstance(ds, xr.core.dataarray.Dataset)


@pytest.mark.xfail
def test_open_netcdf_s3_simplecache():
    bucket = 's3://its-live-data.jpl.nasa.gov'
    key = 'icesat2/alt06/rel003/ATL06_20181230162257_00340206_003_01.h5'
    url = f'simplecache::{bucket}/{key}'
    source = intake.open_netcdf(url,
                                xarray_kwargs=dict(group='gt1l/land_ice_segments', engine='h5netcdf'),
                                storage_options=dict(s3={'anon': True}),
                                )
    ds = source.to_dask()
    assert isinstance(ds._file_obj, xr.backends.h5netcdf_.H5NetCDFStore)
    assert isinstance(ds, xr.core.dataarray.Dataset)


def test_open_opendap():
    url = 'https://www.ncei.noaa.gov/thredds/dodsC/model-cbofs-files/2021/12/nos.cbofs.fields.n001.20211231.t18z.nc'

    source = intake.open_opendap(url, engine='netcdf4', chunks={'time': 1})
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataarray.Dataset)


def test_open_list_opendap():
    url1 = 'https://www.ncei.noaa.gov/thredds/dodsC/model-cbofs-files/2021/12/nos.cbofs.fields.n001.20211231.t18z.nc'
    url2 = 'https://www.ncei.noaa.gov/thredds/dodsC/model-cbofs-files/2021/12/nos.cbofs.fields.n002.20211231.t18z.nc'

    source = intake.open_opendap([url1, url2], engine='netcdf4', chunks={'time': 1})
    ds = source.to_dask()
    assert isinstance(ds, xr.core.dataarray.Dataset)
