# flink-hybrid-source-test

# Test scenarios

## Scenario 1: One file with ordered data. Parallelism=1.

Input data:

```json lines
{"id": 0, "ts": "2023-05-10T12:00:00.000Z", "userId": 1, "value": 100}
{"id": 0, "ts": "2023-05-10T12:00:01.000Z", "userId": 3, "value": 100}
{"id": 0, "ts": "2023-05-10T12:01:00.000Z", "userId": 1, "value": 5}
{"id": 0, "ts": "2023-05-10T12:01:01.000Z", "userId": 3, "value": 1}
{"id": 0, "ts": "2023-05-10T12:02:00.000Z", "userId": 2, "value": 5}
{"id": 0, "ts": "2023-05-10T12:03:00.000Z", "userId": 2, "value": 5}
{"id": 0, "ts": "2023-05-10T12:04:00.000Z", "userId": 2, "value": 5}
{"id": 0, "ts": "2023-05-10T12:05:00.000Z", "userId": 2, "value": 5}
```


In this scenario, all the events are sorted by timestamp so Flink processes them in the right order. There are 2 results emitted.
```
+----+-------------+-------------+
| op |      userId |   sum_5_min |
+----+-------------+-------------+
| +I |           1 |         105 |
| +I |           3 |         101 |
```

The results are correct.


## Scenario 2: One file with unordered data. Parallelism=1.

Input data:

```json lines
{"id": 0, "ts": "2023-05-10T12:00:00.000Z", "userId": 1, "value": 100}
{"id": 2, "ts": "2023-05-10T12:01:00.000Z", "userId": 1, "value": 5}
{"id": 4, "ts": "2023-05-10T12:02:00.000Z", "userId": 2, "value": 5}
{"id": 5, "ts": "2023-05-10T12:03:00.000Z", "userId": 2, "value": 5}
{"id": 6, "ts": "2023-05-10T12:04:00.000Z", "userId": 2, "value": 5}
{"id": 7, "ts": "2023-05-10T12:05:00.000Z", "userId": 2, "value": 5}
{"id": 1, "ts": "2023-05-10T12:00:01.000Z", "userId": 3, "value": 100}
{"id": 3, "ts": "2023-05-10T12:01:01.000Z", "userId": 3, "value": 1}
```

In this scenario, the input file contains exactly the same events, but they are not sorted by the timestamp.
In consequence, when Flink encounters an event that is “late” more than the out-of-orderness defined in
WatermarkStrategy (in our example it is 2 minutes), the event is dropped.

Please note that in this scenario events id=1 and id=3 come after event id=7 with ts=12:05:00, which means that
the watermark is around 12:03:00. In consequence events id=1 and id=3 are considered as late and are dropped.

There is only 1 result emitted.

```
+----+-------------+-------------+
| op |      userId |   sum_5_min |
+----+-------------+-------------+
| +I |           1 |         105 |
```

The results are incorrect.

## Scenario 3: Multiple ordered files. Parallelism=1.

Input data:

`test-data-01.txt`

```json lines
{"id": 0, "ts": "2023-05-10T12:00:00.000Z", "userId": 1, "value": 100}
{"id": 2, "ts": "2023-05-10T12:01:00.000Z", "userId": 1, "value": 5}
{"id": 4, "ts": "2023-05-10T12:02:00.000Z", "userId": 2, "value": 5}
{"id": 5, "ts": "2023-05-10T12:03:00.000Z", "userId": 2, "value": 5}
{"id": 6, "ts": "2023-05-10T12:04:00.000Z", "userId": 2, "value": 5}
{"id": 7, "ts": "2023-05-10T12:05:00.000Z", "userId": 2, "value": 5}
```

test-data-02.txt
```json lines
{"id": 1, "ts": "2023-05-10T12:00:01.000Z", "userId": 3, "value": 100}
{"id": 3, "ts": "2023-05-10T12:01:01.000Z", "userId": 3, "value": 1}
```

If we assume that there are two files, the result might be non-deterministic. It depends on the order in which Flink
reads the files. If Flink reads test-data-01.txt first, then all contents of test-data-02.txt will be dropped since
the watermark has already progressed to 12:03:00.

What is more, even if each file is organized properly, it is hard to ensure correct ordering of files. One can try to
provide its own SplitAssigner which controls the order in which files are processed, but it does not provide
any guarantees in case of transient errors when split processing fails.
