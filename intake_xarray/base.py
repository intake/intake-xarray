class IntakeXarraySourceAdapter:
    container = "xarray"
    name = "xarray"
    version = ""

    def to_dask(self):
        return self.reader(chunks={}).read()

    def __call__(self, *args, **kwargs):
        return self

    get = __call__

    def read(self):
        return self.reader.read()

    discover = read

    read_chunked = to_dask
