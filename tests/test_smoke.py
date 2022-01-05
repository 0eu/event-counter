import io

from contextlib import closing

from core import __version__
from core.__main__ import run_app


EXAMPLE_EVENTS_FILE_PATH = "tests/data/example_events.text"
EXAMPLE_EVENTS_SCHEMA_PATH = "resources/schema.yaml"


def test_version():
    assert __version__ == "0.1.0"


def test_report_is_generated():
    with open(EXAMPLE_EVENTS_FILE_PATH, "r") as r, closing(io.StringIO()) as w:
        run_app(reader=r, writer=w, schema_path=EXAMPLE_EVENTS_SCHEMA_PATH, max_keys_count=10)
        assert (
            w.getvalue() == "2018-01-30 submission_success,1\n2018-02-03 registration_initiated,1\n"
        )
