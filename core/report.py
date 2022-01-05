import collections
import heapq
import io
import json
import logging
import os

from functools import partial
from json import JSONDecodeError
from tempfile import TemporaryFile
from typing import IO, Any, Callable, Iterable, Tuple, Union

from sortedcontainers import SortedDict

from core.schema import Schema, SchemaValidationError


logger = logging.getLogger(__name__)
Pair = collections.namedtuple("Pair", ["date", "event"])


def get_sorted_events_chunks(
    *,
    reader: IO,
    schema: Schema,
    max_keys_count: int,
    split_writer: Callable = partial(TemporaryFile, mode="w+"),
) -> Iterable[io.TextIOBase]:
    """
    Reads the events from the input file and returns a generator of chunks of sorted events.
    It uses a temporary file to store the chunks, if the file is too big to fit in memory.
    The size of the chunks is controlled by the MAX_KEYS_COUNT constant.

    When it's not possible to parse the event or validate its schema, it is skipped and
    logged to the console.

    Chunk is a file-like object that can be used as an iterator.

    :param reader: the input file-like object
    :param schema: schema to validate the events against
    :param max_keys_count: maximum number of keys in the chunk
    :param split_writer: callable that returns a file-like object to write the chunk to
    :return: file-like objects that contains sorted by date, name events
    """
    event_counts = SortedDict()
    for raw_line in reader:
        try:
            key = parse_event(raw_line, schema)
        except (SchemaValidationError, JSONDecodeError) as e:
            logger.warning(f"Validation Error: `{raw_line.strip()}`, Details: `{e.msg}`")
            continue

        event_counts[key] = 1 + event_counts.get(key, 0)

        if len(event_counts) >= max_keys_count:
            yield spill_chunk_of_sorted_event_counts(counts=event_counts, writer=split_writer)

    if event_counts:
        yield spill_chunk_of_sorted_event_counts(counts=event_counts, writer=split_writer)


def parse_event(raw_event: str, schema: Schema) -> Pair:
    """
    Parses the event and validates it against the schema.

    :param raw_event: string representation of the event, it's a json format
    :param schema: a schema to validate the event against
    :return: Pair(date, event) which is used as a key in the sorted dictionary
    """

    def to_date(raw_date: str) -> str:
        sep = "T" if "T" in raw_date else " "
        return raw_date.split(sep=sep, maxsplit=1)[0]

    event = json.loads(raw_event)
    schema.validate(event)
    return Pair(to_date(event["timestamp"]), event["event"])


def spill_chunk_of_sorted_event_counts(*, counts: SortedDict, writer: Callable) -> IO:
    """
    Writes the sorted event counts to a temporary file and returns the file-like object.
    The caller is responsible for closing the file. The file is closed when the context
    manager exits.

    :param counts: a sorted dictionary of event counts
    :param writer: callable that returns a file-like object to write the chunk to
    :return: file-like object that contains sorted by date, name events
    """
    fd = writer()
    while counts:
        key, count = counts.popitem(0)
        fd.write(f"{' '.join(key)},{count}\n")
    fd.seek(os.SEEK_SET)
    return fd


def count_events(*, sorted_chunks: Iterable[Any]) -> Iterable[Tuple[str, int]]:
    """
    Computes the event counts for each event in the chunks. The event counts are merged
    from the sorted chunks and returned as a pair of event key and count.

    Chunks are merged by the heapq.merge() function, which accepts a list of file-like
    objects and returns a generator of pairs of event key and count, sorted by the
    partition key. It read only the first line of each chunk and uses the first line as
    the key.

    :param sorted_chunks: iterable of file-like objects with sorted events
    :return: event key and its count
    """

    def partition_function(text: str, only_key: bool = True) -> Union[str, Tuple[str, int]]:
        """
        Partitions the line into a key and a count.

        :param text: the line of text to extract the key and count from
        :param only_key: whether to return only the key or the key and count
        :return: either the key or a pair of key and count
        """
        k, v = text.rsplit(",", maxsplit=1)
        return k if only_key else k, int(v)

    previous_key = previous_sum = None
    for line in heapq.merge(*sorted_chunks, key=partition_function):
        current_key, current_sum = partition_function(line, only_key=False)

        # if it's the first iteration, set previous key and sum
        if not previous_key:
            previous_key, previous_sum = current_key, int(current_sum)

        # if the current key is equal to the previous one, merge the counts
        elif current_key == previous_key:
            previous_sum += int(current_sum)

        # if the current key is updated, yield the previous key and sum
        else:
            yield previous_key, previous_sum
            previous_key, previous_sum = current_key, int(current_sum)

    if previous_key:
        yield previous_key, previous_sum


def write_report(*, writer: IO, sorted_event_chunks: Iterable[io.TextIOBase]) -> None:
    """
    Writes the report to the report file.

    :param writer: a file-like object to write the report to
    :param sorted_event_chunks: iterable of
    """
    for event_key, count in count_events(sorted_chunks=sorted_event_chunks):
        writer.write(f"{event_key},{count}\n")
