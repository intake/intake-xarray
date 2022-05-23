def _sel(ds, indexers: str):
    """indexers: dict (stored as str) which is passed to xarray.Dataset.sel"""
    return ds.sel(eval(indexers))
