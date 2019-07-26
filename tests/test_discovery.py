import intake
import pytest


def test_discovery():
    with pytest.warns(None) as record:
        registry = intake.autodiscover()
    # For awhile we expect a PendingDeprecationWarning due to
    # do_pacakge_scan=True. But we should *not* get a FutureWarning.
    for record in record.list:
        assert not isinstance(record.message, FutureWarning)
