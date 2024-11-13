class IntakeXarraySourceAdapter:
    container = "xarray"
    name = "xarray"
    version = ""

    def to_dask(self):
        if "chunks" not in self.reader.kwargs:
            return self.reader(chunks={}).read()
        else:
            return self.reader.read()

    def __call__(self, *args, **kwargs):
        return self

    get = __call__

    def read(self):
        return self.reader(chunks=None).read()

    discover = read

    read_chunked = to_dask
