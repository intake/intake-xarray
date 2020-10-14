# intake-xarray

![CI](https://github.com/intake/intake-xarray/workflows/CI/badge.svg)

Intake-xarray: xarray Plugin for [Intake](https://github.com/intake/intake)

See [Intake docs](https://intake.readthedocs.io/en/latest/overview.html) for a general introduction and usage
of Intake and the [intake-xarray docs](https://intake-xarray.readthedocs.io/) for details specific to the
data drivers included in this package.

In `intake-xarray`, there are plugins provided for reading data into [xarray](http://xarray.pydata.org/en/stable/) 
containers:
  - NetCDF
  - OPeNDAP
  - Rasterio
  - Zarr
  - images

and it provides the ability to read xarray data from an Intake server.

### Installation

The conda install instructions are:

```
conda install -c conda-forge intake-xarray
```

To install optional dependencies:

```
conda install -c conda-forge pydap rasterio
```
