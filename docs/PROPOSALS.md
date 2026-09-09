# Proposals

This document records design proposals for Ntry. Add each proposal to the index
and use the same sections: description, motivation, design, advantages,
disadvantages, and open questions.

Statuses are `Proposed`, `Accepted`, `Rejected`, `Implemented`, or `Superseded`.

## Index

| ID | Proposal | Status |
|---|---|---|
| P001 | Fast-mode disk overflow queue | Proposed |

## P001: Fast-Mode Disk Overflow Queue

**Status:** Proposed  
**Created:** 2026-09-09

### Description

Add a bounded disk-backed overflow queue to `fast` durability mode. Ntry would
continue to use its in-memory ingest queue as the primary fast path. When that
queue is full, valid telemetry would move to the slower disk queue instead of
receiving an immediate `429 Too Many Requests` response.

This is an ingest overflow spool, not a dead-letter queue. A dead-letter queue
stores records that cannot be processed. The overflow spool stores normal work
that is waiting for capacity.

### Motivation

In `fast` mode, Ntry acknowledges a request after it enters the bounded
in-memory queue. A burst that fills the 1,024-message queue receives `429`
responses even when the machine has enough disk capacity to process the work
later.

A disk overflow queue would increase burst capacity without increasing the
maximum memory backlog or making HTTP requests wait indefinitely.

### Design

The ingest path would use these rules:

1. Authenticate and validate the Sentry or OTLP request.
2. Try to place its canonical ingest batch in the in-memory queue.
3. If the memory queue is full or a disk backlog already exists, append the
   batch to the disk overflow queue.
4. Return success after the selected acknowledgement policy completes.
5. Return `429` only when the disk queue reaches its configured byte limit.
6. Return a server error if the disk append fails.

Once disk spilling starts, all new batches should use the disk queue until its
backlog is empty. This prevents newer memory entries from overtaking older disk
entries and prevents the disk queue from starving under continuous load.

The writer would drain existing memory entries, consume disk entries in
sequence order, write them to Fjall, and then checkpoint the completed disk
offset. Disk space could be reclaimed after the destination write and
checkpoint are durable.

Each disk entry should contain a format version, sequence number, payload
length, payload, and checksum. On startup, Ntry should truncate an incomplete
final entry and replay entries after the last completed checkpoint. Replay is
at least once, so record writes and related counters must be idempotent.

The disk queue must have a byte limit and leave reserved space for Fjall and
archive writes. Ntry should report queue bytes, entry count, oldest-entry age,
and replay or append failures.

### Acknowledgement Policy

Two policies are possible:

- Buffered append returns success after writing to the operating system. It is
  faster but can lose overflow data after power loss.
- Durable append returns success after a grouped `fsync`. It is slower but lets
  success mean that overflow data can be recovered after a crash.

Durable append is the safer default for spilled batches. Batches accepted by
the primary in-memory path would keep the existing `fast` durability behavior.

### Advantages

- Reduces `429` responses during temporary ingestion bursts.
- Keeps the low-latency in-memory path for normal traffic.
- Bounds memory use while allowing a larger disk-based backlog.
- Can recover spilled batches after a process restart.
- Separates temporary ingress rate from Fjall indexing rate.
- Avoids holding an unlimited number of HTTP connections while waiting for
  memory queue capacity.

### Disadvantages

- Adds a second queue with ordering, replay, checkpoint, and cleanup logic.
- Writes spilled telemetry twice: first to the spool and then to Fjall.
- Uses disk bandwidth and capacity needed by the primary database.
- Creates mixed durability because memory entries and durable spill entries
  have different crash guarantees.
- Requires idempotent replay for records, metrics state, issue updates, and
  diagnostic counters.
- Can increase the delay before spilled telemetry appears in queries and the
  web interface.
- Does not solve sustained overload; a full disk queue must still reject or
  discard new data.

### Open Questions

- Should overflow acknowledgement use buffered append or grouped `fsync`?
- What byte limit and free-space reserve should the spool use?
- Should limits be global or per project?
- How long can an overflow batch wait before it is discarded or rejected?
- How should Ntry handle a batch that repeatedly fails during replay?
- Should a persistent backlog survive normal shutdown or be fully drained?
- Does `fast` remain the correct mode name when some batches are durable?
