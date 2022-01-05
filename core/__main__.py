import functools
import os

from argparse import ArgumentParser
from contextlib import ExitStack, closing
from typing import IO

from core.report import get_sorted_events_chunks, write_report
from core.schema import Schema


MAX_KEYS_COUNT = 1 << 16  # 65_536


def init_argparse() -> ArgumentParser:
    parser = ArgumentParser(
        usage="event-counter [-c number] [-s schema-path] [FILE]",
        description="The application counts events in the file",
    )
    parser.add_argument(
        "--max-keys-count",
        help="Maximum keys to store in-memory while counting",
        type=int,
        default=MAX_KEYS_COUNT,
    )
    parser.add_argument(
        "--schema",
        help="Path to the event validation schema file",
        dest="schema_path",
        type=functools.partial(readable_file, parser),
        default="resources/schema.yaml",
    )
    parser.add_argument(
        "--report",
        dest="report_path",
        help="Report filepath",
        type=str,
        default="report.txt",
    )
    parser.add_argument(
        "--events",
        help="Events filepath",
        dest="events_path",
        type=functools.partial(readable_file, parser),
        required=True,
    )
    return parser


def readable_file(parser: ArgumentParser, filename: str) -> str:
    filename = os.path.join(os.getcwd(), filename)
    if not os.path.exists(filename):
        parser.error(f"File is not readable: {filename}")
    return filename


def run_app(reader: IO, writer: IO, schema_path: str, max_keys_count: int) -> None:
    """
    The entry point of the application.

    :param reader: a file-like object to read events from
    :param writer: a file-like object to write report to
    :param schema_path: a path to the event validation schema file
    :param max_keys_count: maximum keys to store in-memory while counting
    :return: None
    """
    schema = Schema.from_file(schema_path)
    with ExitStack() as stack:
        sorted_event_chunks = [
            stack.enter_context(closing(chunk))
            for chunk in get_sorted_events_chunks(
                reader=reader, schema=schema, max_keys_count=max_keys_count
            )
        ]
        write_report(writer=writer, sorted_event_chunks=sorted_event_chunks)


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    with open(args.events_path, "r") as r, open(args.report_path, "w+") as w:
        run_app(
            reader=r, writer=w, schema_path=args.schema_path, max_keys_count=args.max_keys_count
        )


if __name__ == "__main__":
    main()
