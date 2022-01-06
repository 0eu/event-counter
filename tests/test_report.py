import io
import json
import os
import random

from json import JSONDecodeError
from typing import IO, Callable, List, Tuple, Type

import pytest

from sortedcontainers import SortedDict

from core.report import (
    count_events,
    get_sorted_events_chunks,
    parse_event,
    spill_chunk_of_sorted_event_counts,
)
from core.schema import Field, FieldType, FieldTypes, Schema, SchemaValidationError


@pytest.fixture
def test_event_schema() -> Schema:
    fields = (
        Field(name="timestamp", type=FieldType(type=FieldTypes.str)),
        Field(name="event", type=FieldType(type=FieldTypes.str)),
    )
    yield Schema(fields=fields)


def generate_raw_events_file(n_events: int) -> Tuple[IO, List[dict]]:
    events = [
        {
            "event": random.choice(["a", "b", "c", "d", "e"]),
            "timestamp": f"2020-01-{random.choice(range(10, 15))} 00:00:00.000",
        }
        for _ in range(n_events)
    ]
    fd = io.StringIO("\n".join(json.dumps(event) for event in events))
    fd.seek(os.SEEK_SET)
    return fd, events


def test_parse_valid_event(test_event_schema: Schema) -> None:
    valid_event_data = json.dumps(
        {
            "event": "a",
            "timestamp": "2020-01-01 00:00:00.000",
        }
    )
    key = parse_event(raw_event=valid_event_data, schema=test_event_schema)
    assert key.event == "a" and key.date == "2020-01-01"


@pytest.mark.parametrize(
    "raw_event, exception",
    [
        pytest.param("{a", JSONDecodeError, id="invalid json"),
        pytest.param('{"event": "a", "test": 1}', SchemaValidationError, id="invalid schema"),
    ],
)
def test_parse_invalid_event(
    test_event_schema: Schema, raw_event: str, exception: Type[Exception]
) -> None:
    with pytest.raises(exception):
        parse_event(raw_event=raw_event, schema=test_event_schema)


@pytest.mark.parametrize(
    "n_events, max_keys_count, count_condition",
    (
        pytest.param(100, 3, lambda x: x > 1, id="with max keys count"),
        pytest.param(5, 10, lambda x: x == 1, id="only one sorted chunk"),
        pytest.param(10, 10, lambda x: x == 1, id="only one sorted chunk"),
    ),
)
def test_sorted_events_chunks_should_be_sorted(
    test_event_schema: Schema, n_events: int, max_keys_count: int, count_condition: Callable
) -> None:
    events_fd, events = generate_raw_events_file(n_events=n_events)
    sorted_chunks = list(
        get_sorted_events_chunks(
            reader=events_fd,
            schema=test_event_schema,
            max_keys_count=max_keys_count,
            split_writer=io.StringIO,
        )
    )
    events_fd.close()

    # Check that the chunks are sorted
    for chunk in sorted_chunks:
        lines = [line.strip().rsplit(",", maxsplit=1)[0] for line in chunk]
        lines.sort(key=lambda x: x.split(" "))
        chunk.seek(os.SEEK_SET)
        for index, line in enumerate(chunk):
            assert line.strip().rsplit(",", maxsplit=1)[0] == lines[index]
        chunk.close()

    # Check that the chunks are not exceeding the max keys count
    assert count_condition(len(sorted_chunks))


@pytest.mark.parametrize(
    "events, expected_content",
    (
        pytest.param(
            ["a", "b", "c", "d", "e", "a", "b"], "a,2\nb,2\nc,1\nd,1\ne,1\n", id="with duplicates"
        ),
        pytest.param(["a", "b", "c"], "a,1\nb,1\nc,1\n", id="without duplicates"),
    ),
)
def test_chunk_of_sorted_event_counts_spilled(events: List[str], expected_content: str) -> None:
    counts = SortedDict()
    for event in events:
        counts[event] = counts.get(event, 0) + 1

    writer = spill_chunk_of_sorted_event_counts(counts=counts, writer=io.StringIO)
    assert writer.getvalue() == expected_content  # noqa
    writer.close()


@pytest.mark.parametrize(
    "chunks, expected_counts",
    (
        pytest.param(
            [
                ["2021-01-01,a,1", "2021-01-01,b,1"],
                ["2021-01-01,a,1", "2021-01-02,c,1"],
            ],
            [("2021-01-01,a", 2), ("2021-01-01,b", 1), ("2021-01-02,c", 1)],
            id="with merge",
        ),
        pytest.param(
            [
                ["2021-01-01,a,1", "2021-01-01,b,1"],
                ["2021-01-02,a,1", "2021-01-02,c,1"],
            ],
            [("2021-01-01,a", 1), ("2021-01-01,b", 1), ("2021-01-02,a", 1), ("2021-01-02,c", 1)],
            id="without merge",
        ),
        pytest.param(
            [["2021-01-01,a,1"]],
            [("2021-01-01,a", 1)],
            id="only one event",
        ),
        pytest.param([], [], id="no events"),
    ),
)
def test_count_events(chunks: List[List[str]], expected_counts: List[Tuple[str, int]]) -> None:
    assert list(count_events(sorted_chunks=chunks)) == expected_counts
