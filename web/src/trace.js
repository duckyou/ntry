// @ts-check

/**
 * @template {{ id: string }} T
 * @param {(cursor: string | null) => Promise<{ items: T[], next_cursor: string | null }>} fetchPage
 * @returns {Promise<T[]>}
 */
export async function loadAllPages(fetchPage) {
  const records = [];
  let cursor = null;
  do {
    const page = await fetchPage(cursor);
    records.push(...page.items);
    cursor = page.next_cursor;
  } while (cursor);
  return records;
}

/** @typedef {{ id: string, timestamp_unix_nano: number, fields: Record<string, unknown> }} TraceRecord */

/**
 * @template {TraceRecord} T
 * @param {T[]} records
 * @returns {{ record: T, depth: number }[]}
 */
export function traceRows(records) {
  const unique = [...new Map(records.map((record) => [record.id, record])).values()];
  const spanId = (/** @type {T} */ record) => typeof record.fields.span_id === "string" ? record.fields.span_id : null;
  const spanIds = new Set(unique.map(spanId).filter(Boolean));
  /** @type {Map<string | null, T[]>} */
  const children = new Map();
  for (const record of unique) {
    const parent = typeof record.fields.parent_span_id === "string" ? record.fields.parent_span_id : null;
    const key = parent && spanIds.has(parent) ? parent : null;
    const group = children.get(key);
    if (group) group.push(record);
    else children.set(key, [record]);
  }
  const chronological = (/** @type {T} */ a, /** @type {T} */ b) =>
    a.timestamp_unix_nano - b.timestamp_unix_nano || a.id.localeCompare(b.id);
  for (const group of children.values()) group.sort(chronological);

  /** @type {{ record: T, depth: number }[]} */
  const rows = [];
  const seen = new Set();
  const visit = (/** @type {T} */ record, /** @type {number} */ depth) => {
    if (seen.has(record.id)) return;
    seen.add(record.id);
    rows.push({ record, depth });
    for (const child of children.get(spanId(record)) || []) visit(child, depth + 1);
  };
  for (const root of children.get(null) || []) visit(root, 0);
  for (const record of unique.sort(chronological)) visit(record, 0);
  return rows;
}
