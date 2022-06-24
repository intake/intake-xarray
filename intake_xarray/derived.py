from intake import Schema
from intake.source.derived import GenericTransform


class XArrayTransform(GenericTransform):
    """Transform where the input and output are both xarray objects.
    You must supply ``transform`` and any ``transform_kwargs``.
    """

    input_container = "xarray"
    container = "xarray"
    optional_params = {}
    _ds = None

    def to_dask(self):
        if self._ds is None:
            self._pick()
            self._ds = self._transform(
                self._source.to_dask(), **self._params["transform_kwargs"]
            )
        return self._ds

    def _get_schema(self):
        """load metadata only if needed"""
        self.to_dask()
        return Schema(
            datashape=None,
            dtype=None,
            shape=None,
            npartitions=None,
            extra_metadata=self._ds.extra_metadata,
        )

    def read(self):
        return self.to_dask().compute()


class Sel(XArrayTransform):
    """Simple array transform to subsample an xarray object using
    the sel method.
    Note that you could use XArrayTransform directly, by writing a
    function to choose the subsample instead of a method as here.
    """

    input_container = "xarray"
    container = "xarray"
    required_params = ["indexers"]

    def __init__(self, indexers, **kwargs):
        """
        indexers: dict (stord as str) which is passed to xarray.Dataset.sel
        """
        # this class wants required "indexers", but XArrayTransform
        # uses "transform_kwargs", which we don't need since we use a method for the
        # transform
        kwargs.update(
            transform=self.sel,
            indexers=indexers,
            transform_kwargs={},
        )
        super().__init__(**kwargs)

    def sel(self, ds):
        return ds.sel(eval(self._params["indexers"]))
