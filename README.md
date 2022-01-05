# Event Counter

The event counter is an event counter that validates events by user-defined schema, also it might be used for monitoring.

## Schema Validator
A schema validator is a function that validates an event against a schema. The schema is defined by the user in the configuration file: `resources/schema.yaml`.

The application is flexible, thus can load the schema from a file by provided the path to the file:
```shell
python -m core --schema resources/custom-schema.yaml
```

### Schema definition
Schema must have a `kind: Schema` in the header, thus it might be used for further improvements.

The schema is defined by the `schema` key:
```yaml
kind: Schema

schema:
  - name: event_id
    type: string
    required: true
```

Supported types:
- **str**: string
- **int**: integer
- **bool**: boolean
- **enum**: a finite set of variants

Fields:
- **name**: name of the field
- **type**: type of the field
- **required**: whether the field is required
- **description**: business description of the field
- **variants**: variants of the field in case of enum

In case an event's schema does not match the schema defined in the configuration file, the event is rejected by throwing exception `SchemaValidationError`.

Events may be filtered out in the following circumstances:
1. The event's schema does not match the schema defined in the configuration file.
2. The JSON is of an event is not parseable.

## Report Generation
The report consists of the following columns:
- **event_date**: date of the event in the format (YYYY-mm-dd)
- **event_name**: name of the event
- **count**: number of events happened on the date

The file with events is too large to fit in memory. Let's consider the possible solutions.

### Solution 1: Hash Table on the single machine
The solution is to use a hash table to store the events. The hash table is a map from the event name to the number of events happened on the date. The main assumption here is that the number of pairs (event_date, event_name) events is small, thus it may fit to RAM.

But what if not? The hash table is not scalable. Consider the following scenario:
- The number of events is large, say 100,000,000.
- The number of unique keys is also large, say 100,000,000.
- A single machine has only 512MB of RAM.
- We can't store all the keys in the hash table, due to OOM.

### Solution 2: Distributed Hash Table on multiple machines
The idea is the same as Solution 1, but the hash table is distributed across multiple machines. We can write a custom logic for sharding event keys across those machines and then generate the report by writing counts from each machine to a file.

### Solution 3: Map Reduce Like Approach [Implemented Solution]
The idea is to split the events into chunks and then apply the map-reduce approach to each chunk. The map-reduce approach is a common way to process large data sets.

The number of rows that written to a chunk is controlled by the `max_keys_count` parameter. It represents the maximum number of keys that can be stored in hash table before it will be spilled to disk.

The map function is responsible for splitting the data into chunks and the reduce function is responsible for aggregating, merging the results.

#### Map
Map function goes through events, validates them, and if they are valid, it increments counter in the hash table. The default hash table does not support ordering by key on insertion, so there's a `sortedcontainer` lib that provides a sorted map implemented with the balanced tree.
Once, the hash table is full, it spills to disk.

#### Reduce
For merging the results from different files there's a `merge` function of a heapq library. It provides a convenient and efficient way to merge the results from temporary files by using heap data structure. Finally, there's a report with counts of events at `output/report.txt`.

The report file path can be adjusted by changing an option: `--report`.

### Solution 4: Probabilistic Approach [Not a solution of a problem but an interesting approach]
In computing, the count–min sketch (CM sketch) is a probabilistic data structure that serves as a frequency table of events in a stream of data. It uses hash functions to map events to frequencies, but unlike a hash table uses only sub-linear space, at the expense of overcounting some events due to collisions. More info [here](https://en.wikipedia.org/wiki/Count–min_sketch).

## Installation

To build a docker image:
```shell
$ make install
```

To run tests withing the docker container:
```shell
$ make test
```

To generate a report:
```shell
$ make report FILENAME="example_events.text"
docker run ... --report output/report.txt

$ cat output/report.txt | head
2018-01-30 submission_success,1
2018-02-03 registration_initiated,1
```
where `FILENAME` is the name of the file with events, e.g. `example_events.text`. If you want to test the application against a custom file, put it to `tests/data/` directory, because it's mounted as a read-only bind for the container.

The report is generated in `output/report.txt`.
