import assert from "node:assert/strict";
import test from "node:test";
import { loadAllPages, traceRows } from "../src/trace.js";

test("loads and arranges a complete trace", async () => {
  const span = (id, spanId, parent, time) => ({
    id,
    timestamp_unix_nano: time,
    fields: { span_id: spanId, ...(parent ? { parent_span_id: parent } : {}) },
  });
  const root = span("root", "a", null, 10);
  const late = span("late", "c", "a", 30);
  const early = span("early", "b", "a", 20);
  const grandchild = span("grandchild", "d", "b", 25);
  const orphan = span("orphan", "e", "missing", 5);
  const disconnectedA = span("cycle-a", "f", "g", 40);
  const disconnectedB = span("cycle-b", "g", "f", 50);
  const pages = [
    { items: [late, root, early], next_cursor: "page-2" },
    { items: [root, grandchild, orphan, disconnectedB], next_cursor: "page-3" },
    { items: [disconnectedA], next_cursor: null },
  ];
  const cursors = [];
  const records = await loadAllPages(async (cursor) => {
    cursors.push(cursor);
    return pages[cursors.length - 1];
  });
  const rows = traceRows([...records, early]);

  assert.deepEqual(cursors, [null, "page-2", "page-3"]);
  assert.equal(records.length, 7);
  assert.deepEqual(rows.map(({ record, depth }) => [record.id, depth]), [
    ["orphan", 0],
    ["root", 0],
    ["early", 1],
    ["grandchild", 2],
    ["late", 1],
    ["cycle-a", 0],
    ["cycle-b", 1],
  ]);
  assert.equal(new Set(rows.map(({ record }) => record.id)).size, rows.length);
  await assert.rejects(
    loadAllPages(async () => ({ items: [], next_cursor: "repeated" })),
    /repeated a cursor/,
  );
});
