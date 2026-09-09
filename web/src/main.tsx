import { FormEvent, startTransition, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { loadAllPages, traceRows } from "./trace.js";
import "./style.css";

type Project = { id: number; name: string; key: string; status: "active" | "deleting" };
type Issue = {
  id: string;
  project_id: number;
  title: string;
  level: string;
  first_seen_ms: number;
  last_seen_ms: number;
  total_seen: number;
  retained_events: number;
  latest_record_id: string | null;
};
type Signal = "error" | "log" | "metric" | "trace";
type Record = {
  id: string;
  signal: Signal;
  source: string;
  source_id: string | null;
  issue_id: string | null;
  timestamp_unix_nano: number;
  received_at_ms: number;
  fields: globalThis.Record<string, unknown>;
  raw: unknown;
};
type Status = {
  records: number;
  disk_bytes: number;
  storage_warning: boolean;
  listeners: { web_api: number; sentry: number | null; ddtrace: number | null; otlp: number | null };
  process: { accepted: number; malformed: number; failed: number };
};
type Page<T> = { items: T[]; next_cursor: string | null };
type Tab = "issues" | "logs" | "metrics" | "traces";
type Theme = "dark" | "light" | "system";
type StaticPage = "settings" | "docs" | null;
type View = { projectId: number; tab: Tab; range: keyof typeof ranges; query: string; items: (Issue | Record)[] };
type Change = { project_id: number; signal: Signal };
type IssueEvent = { issueId: string; records?: Record[]; record?: Record; error?: string };
type WebRoute = { projectId: number | null; tab: Tab; issueId: string | null; range: keyof typeof ranges; query: string };
type StackFrame = {
  filename?: string;
  path?: string;
  function?: string;
  line?: number;
  in_app?: boolean;
  context_line?: string;
  pre_context?: string[];
  post_context?: string[];
  variables?: unknown;
};
type ExceptionValue = {
  type?: string;
  message?: string;
  mechanism?: string;
  handled?: boolean;
  frames?: StackFrame[];
  stacktrace?: string;
};

const ranges = { "15m": 15 * 60_000, "1h": 60 * 60_000, "24h": 24 * 60 * 60_000 };
const tabs: Tab[] = ["issues", "logs", "metrics", "traces"];
const tabSignals: globalThis.Record<Tab, Signal> = { issues: "error", logs: "log", metrics: "metric", traces: "trace" };
const themeOptions: { value: Theme; label: string; detail: string }[] = [
  { value: "dark", label: "dark", detail: "rosé pine" },
  { value: "light", label: "light", detail: "rosé pine dawn" },
  { value: "system", label: "system", detail: "follow operating system" },
];

function readTheme(): Theme {
  const value = localStorage.getItem("ntry-theme");
  return value === "dark" || value === "light" ? value : "system";
}

function applyTheme(theme: Theme) {
  const resolved = theme === "system"
    ? matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"
    : theme;
  document.documentElement.dataset.theme = resolved;
  document.documentElement.style.colorScheme = resolved;
  document.querySelector<HTMLMetaElement>('meta[name="theme-color"]')?.setAttribute("content", resolved === "dark" ? "#191724" : "#faf4ed");
}

const initialTheme = readTheme();
applyTheme(initialTheme);

function readRoute(): WebRoute {
  const match = location.pathname.match(/^\/projects\/(\d+)\/(issues|logs|metrics|traces)(?:\/([^/]+))?\/?$/);
  const params = new URLSearchParams(location.search);
  const requestedRange = params.get("range");
  const tab = (match?.[2] as Tab | undefined) || "issues";
  return {
    projectId: match ? Number(match[1]) : null,
    tab,
    issueId: tab === "issues" && match?.[3] ? decodeURIComponent(match[3]) : null,
    range: requestedRange && requestedRange in ranges ? requestedRange as keyof typeof ranges : "1h",
    query: params.get("query") || "",
  };
}

function routePath(projectId: number, tab: Tab, issueId: string | null, range: keyof typeof ranges, query: string) {
  const params = new URLSearchParams();
  if (range !== "1h") params.set("range", range);
  if (query) params.set("query", query);
  const search = params.toString();
  return `/projects/${projectId}/${tab}${issueId ? `/${encodeURIComponent(issueId)}` : ""}${search ? `?${search}` : ""}`;
}

const initialRoute = readRoute();

async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...options,
    headers: {
      ...(options?.body ? { "Content-Type": "application/json" } : {}),
      ...options?.headers,
    },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || `${response.status} ${response.statusText}`);
  }
  return response.status === 204 ? (undefined as T) : response.json();
}

const ago = (ms: number) => {
  const seconds = Math.max(0, Math.floor((Date.now() - ms) / 1_000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)}h ago`;
  return `${Math.floor(seconds / 86_400)}d ago`;
};

const bytes = (value: number) => {
  if (value < 1_024) return `${value} B`;
  if (value < 1_048_576) return `${(value / 1_024).toFixed(1)} KiB`;
  if (value < 1_073_741_824) return `${(value / 1_048_576).toFixed(1)} MiB`;
  return `${(value / 1_073_741_824).toFixed(1)} GiB`;
};

const fieldString = (record: Record, name: string) => typeof record.fields[name] === "string" ? record.fields[name] as string : null;
const fieldNumber = (record: Record, name: string) => typeof record.fields[name] === "number" ? record.fields[name] as number : null;
const text = (record: Record) => fieldString(record, "message") || fieldString(record, "title") || fieldString(record, "name") || record.id;
const level = (record: Record) => fieldString(record, "level") || "info";
const metricValue = (record: Record) => fieldNumber(record, "value") || 0;
const metricName = (record: Record) => fieldString(record, "name") || "unnamed";
const metricType = (record: Record) => fieldString(record, "type") || "metric";
const metricUnit = (record: Record) => fieldString(record, "unit") || "value";
const recordTimeMs = (record: Record) => record.timestamp_unix_nano / 1_000_000;
const traceStatus = (record: Record) => fieldString(record, "span.status") || "unset";
const traceDuration = (record: Record) => {
  const nanoseconds = fieldNumber(record, "duration_ns");
  if (nanoseconds === null) return "--";
  if (nanoseconds < 1_000) return `${nanoseconds} ns`;
  if (nanoseconds < 1_000_000) return `${(nanoseconds / 1_000).toFixed(1)} us`;
  if (nanoseconds < 1_000_000_000) return `${(nanoseconds / 1_000_000).toFixed(1)} ms`;
  return `${(nanoseconds / 1_000_000_000).toFixed(2)} s`;
};
const ingestUrl = (port: number | null | undefined, path: string, username = "") => {
  if (!port) return null;
  const url = new URL(path, location.origin);
  url.port = String(port);
  url.username = username;
  return url.toString();
};

async function copyText(value: string) {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch {
    // Plain HTTP and denied clipboard permissions use the fallback below.
  }
  const input = document.createElement("textarea");
  input.value = value;
  input.readOnly = true;
  input.style.position = "fixed";
  input.style.left = "-9999px";
  document.body.append(input);
  input.select();
  try {
    return document.execCommand("copy");
  } catch {
    return false;
  } finally {
    input.remove();
  }
}

function Bars({ records }: { records: Record[] }) {
  const values = records.slice(0, 24).reverse().map(metricValue);
  const max = Math.max(...values.map(Math.abs), 1);
  return (
    <div className="metric-bars" aria-label={`Recent values: ${values.join(", ")}`}>
      {values.map((value, index) => (
        <i key={index} className="metric-bar" style={{ height: `${Math.max(6, (Math.abs(value) / max) * 100)}%` }} />
      ))}
    </div>
  );
}

const pairs = (value: unknown): [string, unknown][] => {
  if (Array.isArray(value)) return value.filter((item): item is [string, unknown] => Array.isArray(item) && item.length >= 2);
  return value && typeof value === "object" ? Object.entries(value) : [];
};
const prefixedPairs = (fields: globalThis.Record<string, unknown>, prefix: string): [string, unknown][] => Object.entries(fields)
  .filter(([name]) => name.startsWith(prefix))
  .map(([name, value]) => [name.slice(prefix.length), value]);

const display = (value: unknown) => typeof value === "string" ? value : JSON.stringify(value, null, 2);
const queryLiteral = (value: string | number | boolean) => JSON.stringify(value);
const displayTime = (value: unknown) => {
  if (typeof value === "number") return new Date(value < 1_000_000_000_000 ? value * 1_000 : value).toLocaleString();
  if (typeof value === "string" && !Number.isNaN(Date.parse(value))) return new Date(value).toLocaleString();
  return String(value || "--");
};

function InfoCard({ title, values, collapsible = false, actions = false, onAddQuery }: {
  title: string;
  values: [string, unknown][];
  collapsible?: boolean;
  actions?: boolean;
  onAddQuery?: (section: string, name: string, value: string | number | boolean) => void;
}) {
  const [copied, setCopied] = useState("");
  const present = values.filter(([, value]) => value !== undefined && value !== null && value !== "");
  if (!present.length) return null;
  const content = <dl>
    {present.map(([name, value]) => (
      <div key={name}>
        <dt>{name}</dt>
        <dd>
          <span>{display(value)}</span>
          {actions && (
            <span className="value-actions">
              <button
                type="button"
                aria-label={`Add ${name} to query`}
                title={typeof value === "string" || typeof value === "number" || typeof value === "boolean" ? "Add to query" : "Only scalar values can be queried"}
                disabled={typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean"}
                onClick={() => onAddQuery?.(title, name, value as string | number | boolean)}
              >+</button>
              <button
                type="button"
                aria-label={`Copy ${name}`}
                title="Copy value"
                onClick={async () => {
                  if (await copyText(display(value) || "")) {
                    setCopied(name);
                    setTimeout(() => setCopied(""), 1_200);
                  }
                }}
              >{copied === name ? "OK" : "CP"}</button>
            </span>
          )}
        </dd>
      </div>
    ))}
  </dl>;
  return collapsible ? (
    <details className="issue-side-card" open><summary>// {title}</summary>{content}</details>
  ) : (
    <section className="issue-side-card"><h3>// {title}</h3>{content}</section>
  );
}

const recordAttributes = (record: Record) => prefixedPairs(record.fields, "attribute.");

function TraceTree({ records, selectedId, onSelect }: { records: Record[]; selectedId: string; onSelect: (record: Record) => void }) {
  return <div className="trace-tree">
    {traceRows(records).map(({ record, depth }) => (
      <button className={record.id === selectedId ? "active" : ""} style={{ paddingLeft: `${10 + depth * 18}px` }} key={record.id} onClick={() => onSelect(record)}>
        <span className={`level ${traceStatus(record)}`}>{traceStatus(record).toUpperCase()}</span>
        <span>{fieldString(record, "name") || "unnamed span"}</span>
        <small>{traceDuration(record)}</small>
      </button>
    ))}
  </div>;
}

function RecordDetail({ record, traceRecords, traceLoading, traceError, onSelectTrace, onClose, onAddQuery }: {
  record: Record;
  traceRecords: Record[];
  traceLoading: boolean;
  traceError: string;
  onSelectTrace: (record: Record) => void;
  onClose: () => void;
  onAddQuery: (section: string, name: string, value: string | number | boolean) => void;
}) {
  const attributes = recordAttributes(record);
  const isMetric = record.signal === "metric";
  const isTrace = record.signal === "trace";
  const title = isMetric ? metricName(record) : text(record);
  const logger = fieldString(record, "logger") || fieldString(record, "resource.service.name") || record.fields["attribute.logger.name"];
  const loggerField = fieldString(record, "logger") ? "logger" : fieldString(record, "resource.service.name") ? "resource.service.name" : "attribute.logger.name";
  const [view, setView] = useState<"view" | "raw">("view");
  const addRecordQuery = (section: string, name: string, value: string | number | boolean) => {
    const rawValue = name === "timestamp" ? record.timestamp_unix_nano : name === "received" ? record.received_at_ms : value;
    if (typeof rawValue === "string" || typeof rawValue === "number" || typeof rawValue === "boolean") {
      onAddQuery(section, name === "logger" ? loggerField : name, rawValue);
    }
  };
  return (
    <aside className="detail-drawer record-drawer" aria-label={`${record.signal} detail`}>
      <div className="detail-head"><span>// {isMetric ? "METRIC_SAMPLE" : isTrace ? "TRACE_SPAN" : "LOG_EVENT"}</span><button aria-label="Close detail" onClick={onClose}>[ESC]</button></div>
      <div className="record-tabs" role="tablist" aria-label="Record representation">
        <button role="tab" aria-selected={view === "view"} className={view === "view" ? "active" : ""} onClick={() => setView("view")}>VIEW</button>
        <button role="tab" aria-selected={view === "raw"} className={view === "raw" ? "active" : ""} onClick={() => setView("raw")}>RAW</button>
      </div>
      {view === "view" ? <>
        <header className="record-hero">
          <div className="record-kicker">
            <span className={`level ${isTrace ? traceStatus(record) : level(record)}`}>{(isMetric ? metricType(record) : isTrace ? traceStatus(record) : level(record)).toUpperCase()}</span>
            <time>{new Date(recordTimeMs(record)).toLocaleString()} :: received {ago(record.received_at_ms)}</time>
          </div>
          <h2>{title}</h2>
          {isMetric && (
            <div className="record-metric-value">
              <strong>{metricValue(record).toLocaleString()}</strong>
              <span>{metricUnit(record)}</span>
            </div>
          )}
          {isTrace && <div className="record-metric-value"><strong>{traceDuration(record)}</strong><span>{fieldString(record, "resource.service.name") || record.source}</span></div>}
        </header>

        <div className="record-cards">
          <InfoCard actions collapsible onAddQuery={addRecordQuery} title="EVENT" values={[
            ["record_id", record.id],
            ["source", record.source],
            ["source_id", record.source_id],
            ["issue", record.issue_id],
            ["timestamp", new Date(recordTimeMs(record)).toISOString()],
            ["received", new Date(record.received_at_ms).toISOString()],
            ["level", isMetric || isTrace ? undefined : level(record)],
            ["logger", isTrace ? undefined : logger],
            ["trace_id", fieldString(record, "trace_id")],
          ]} />
          <InfoCard actions collapsible onAddQuery={addRecordQuery} title="ATTRIBUTES" values={attributes} />
          {isMetric && <InfoCard actions collapsible onAddQuery={addRecordQuery} title="METRIC" values={[
            ["name", metricName(record)],
            ["type", metricType(record)],
            ["value", record.fields.value],
            ["unit", record.fields.unit],
          ]} />}
          {isTrace && <InfoCard actions collapsible onAddQuery={addRecordQuery} title="TRACE" values={[
            ["trace_id", record.fields.trace_id],
            ["span_id", record.fields.span_id],
            ["parent_span_id", record.fields.parent_span_id],
            ["name", record.fields.name],
            ["span.kind", record.fields["span.kind"]],
            ["span.status", record.fields["span.status"]],
            ["span.status_message", record.fields["span.status_message"]],
            ["duration_ns", record.fields.duration_ns],
            ["resource.service.name", record.fields["resource.service.name"]],
          ]} />}
          {isTrace && <section className="issue-side-card trace-card"><h3>// TRACE TREE</h3>
            {traceLoading ? <p>loading trace spans_</p> : traceError ? <p>! {traceError}</p> : <TraceTree records={traceRecords} selectedId={record.id} onSelect={onSelectTrace} />}
          </section>}
        </div>
      </> : (
        <pre className="record-raw-code">{JSON.stringify(record.raw, null, 2)}</pre>
      )}
    </aside>
  );
}

function FrameVariables({ value }: { value: unknown }) {
  const [view, setView] = useState<"view" | "raw">("view");
  const variables = pairs(value);
  return (
    <details className="frame-vars">
      <summary>LOCAL VARIABLES <small>{variables.length ? `[${variables.length}]` : "[RAW]"}</small></summary>
      <div className="variable-tabs" role="tablist" aria-label="Local variables representation">
        <button role="tab" aria-selected={view === "view"} className={view === "view" ? "active" : ""} onClick={() => setView("view")}>VIEW</button>
        <button role="tab" aria-selected={view === "raw"} className={view === "raw" ? "active" : ""} onClick={() => setView("raw")}>RAW</button>
      </div>
      {view === "view" && variables.length ? (
        <dl className="variable-list">
          {variables.map(([name, variable]) => <div key={name}><dt>{name}</dt><dd>{display(variable)}</dd></div>)}
        </dl>
      ) : (
        <pre>{JSON.stringify(value, null, 2)}</pre>
      )}
    </details>
  );
}

function StackFrameCard({ frame }: { frame: StackFrame }) {
  const before = Array.isArray(frame.pre_context) ? frame.pre_context.filter((line): line is string => typeof line === "string") : [];
  const after = Array.isArray(frame.post_context) ? frame.post_context.filter((line): line is string => typeof line === "string") : [];
  const line = typeof frame.line === "number" ? frame.line : before.length + 1;
  const path = `${frame.filename || frame.path || "unknown"}:${line}`;
  const source = [...before, ...(frame.context_line ? [frame.context_line] : []), ...after];
  const firstLine = line - before.length;
  const [copyStatus, setCopyStatus] = useState<"idle" | "copied" | "error">("idle");

  async function copyPath() {
    const copied = await copyText(path);
    setCopyStatus(copied ? "copied" : "error");
    setTimeout(() => setCopyStatus("idle"), 1_200);
  }

  const frameHeader = <>
    <span>{frame.in_app ? "APP" : "LIB"}</span>
    <strong>{frame.function || "<module>"}</strong>
    <div className="frame-path">
      <code>{path}</code>
      <button
        aria-label={`Copy ${path}`}
        title="Copy path with line number"
        onClick={(event) => { event.preventDefault(); event.stopPropagation(); copyPath(); }}
      >
        {copyStatus === "idle" ? "CP" : copyStatus === "copied" ? "OK" : "ERR"}
      </button>
    </div>
  </>;

  return (
    <article className={`stack-frame ${frame.in_app ? "in-app" : ""}`}>
      {source.length > 0 ? (
        <details className="source-block" open={frame.in_app === true}>
          <summary className="frame-summary">{frameHeader}</summary>
          <pre className="source-code">{source.map((text, offset) => {
            const number = firstLine + offset;
            return <span className={number === line ? "active" : ""} key={number}><i>{number}</i>{text}</span>;
          })}</pre>
          {frame.variables != null && <FrameVariables value={frame.variables} />}
        </details>
      ) : <>
        <div className="frame-summary no-source">{frameHeader}</div>
        {frame.variables != null && <FrameVariables value={frame.variables} />}
      </>}
    </article>
  );
}

function StackTrace({ frames = [] }: { frames?: StackFrame[] }) {
  const groups = frames.reduce<StackFrame[][]>((groups, frame) => {
    const previous = groups.at(-1);
    if (!frame.in_app && previous && !previous[0].in_app) previous.push(frame);
    else groups.push([frame]);
    return groups;
  }, []);

  return (
    <div className="stacktrace">
      {groups.map((group, groupIndex) => group.length > 1 && !group[0].in_app ? (
        <details className="library-frames" key={groupIndex}>
          <summary className="frame-summary"><span>LIB</span><strong>{group.length} collapsed library frames</strong></summary>
          {group.map((frame, frameIndex) => (
            <StackFrameCard frame={frame} key={`${frame.filename}-${frame.line}-${frameIndex}`} />
          ))}
        </details>
      ) : (
        <StackFrameCard frame={group[0]} key={`${group[0].filename}-${group[0].line}-${groupIndex}`} />
      ))}
      {!frames.length && <p className="detail-empty">No stack trace was captured.</p>}
    </div>
  );
}

function IssuePage({ issue, event, onSelectEvent, onBack }: { issue: Issue; event: IssueEvent | null; onSelectEvent: (id: string) => void; onBack: () => void }) {
  const fields = event?.record?.fields || {};
  const exceptions = (Array.isArray(fields["error.exceptions"])
    ? fields["error.exceptions"].filter((value): value is ExceptionValue => !!value && typeof value === "object")
    : []);
  const currentException = exceptions[0];
  const breadcrumbs = Array.isArray(fields.breadcrumbs)
    ? fields.breadcrumbs.filter((value): value is globalThis.Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value))
    : [];
  const http = prefixedPairs(fields, "http.");
  const requestBody = fields["http.body"] ?? fields["http.data"];
  const textualStacktrace = typeof currentException?.stacktrace === "string"
    ? currentException.stacktrace
    : typeof fields["error.stacktrace"] === "string" ? fields["error.stacktrace"] : null;
  const trace: [string, unknown][] = [
    ["trace_id", fields.trace_id],
    ["span_id", fields.span_id],
    ["parent_span_id", fields.parent_span_id],
    ...prefixedPairs(fields, "trace.").filter(([name]) => !["trace_id", "span_id", "parent_span_id"].includes(name)),
  ];

  return (
    <div className="issue-page">
      <button className="issue-back" onClick={onBack}>&lt;- BACK_TO_ISSUES</button>
      <header className="issue-hero">
        <div>
          <span className={`level ${issue.level}`}>{issue.level.toUpperCase()}</span>
          <p>{exceptions[0]?.type || String(fields["error.type"] || "ISSUE")} / {issue.id.slice(0, 12)}</p>
          <h1>{issue.title}</h1>
        </div>
        <div className="issue-frequency">
          <strong>{issue.total_seen.toLocaleString()}</strong>
          <span>TOTAL EVENTS</span>
        </div>
      </header>

      <div className="issue-timeline">
        <span>FIRST SEEN <strong>{new Date(issue.first_seen_ms).toLocaleString()}</strong></span>
        <i />
        <span>LAST SEEN <strong>{new Date(issue.last_seen_ms).toLocaleString()} ({ago(issue.last_seen_ms)})</strong></span>
        <i />
        <span>RETAINED <strong>{issue.retained_events}</strong></span>
      </div>

      {event?.record && event.records && event.records.length > 1 && (
        <label className="issue-event-picker">
          <span>OBSERVING EVENT</span>
          <select value={event.record.id} onChange={(change) => onSelectEvent(change.target.value)}>
            {event.records.map((record) => (
              <option value={record.id} key={record.id}>{displayTime(recordTimeMs(record))} :: {(record.source_id || record.id).slice(0, 12)}</option>
            ))}
          </select>
        </label>
      )}

      {!issue.retained_events ? (
        <div className="issue-page-empty">[ NO_RETAINED_EVENT ]<small>The issue aggregate remains, but its event payload was removed by retention.</small></div>
      ) : !event?.records && !event?.error ? (
        <div className="issue-page-empty">&gt; loading events_</div>
      ) : event?.error ? (
        <div className="issue-page-empty">! {event.error}</div>
      ) : !event?.record ? (
        <div className="issue-page-empty">[ NO_RETAINED_EVENT ]<small>No retained event payload was found for this issue.</small></div>
      ) : event?.record ? (
        <div className="issue-detail-grid">
          <div className="issue-primary">
            {exceptions.length ? (
              <details className="event-section" open>
                <summary className="event-section-head">
                  <span>// STACK TRACE</span>
                  <small>{textualStacktrace ? "OTLP TEXT" : `${currentException.frames?.length || 0} frames`} {exceptions.length > 1 ? `:: ${exceptions.length - 1} causes` : ""}</small>
                </summary>
                <div className="stack-exception-head">
                  <div>
                    <strong>{currentException.type || "Error"}</strong>
                    <p>{currentException.message || issue.title}</p>
                  </div>
                  <span>{typeof currentException.handled !== "boolean" ? "HANDLING UNKNOWN" : currentException.handled ? "HANDLED" : "UNHANDLED"} :: {currentException.mechanism || "unknown mechanism"}</span>
                </div>
                {textualStacktrace ? <pre className="payload">{textualStacktrace}</pre> : <StackTrace frames={currentException.frames} />}
                {exceptions.length > 1 && (
                  <div className="exception-causes">
                    {exceptions.slice(1).map((exception, exceptionIndex) => (
                    <details className="exception-stack" key={`${exception.type}-${exceptionIndex}`}>
                      <summary>
                        <span>CAUSE {exceptionIndex + 1}</span>
                        <strong>{exception.type || "Error"}</strong>
                        <small>{exception.frames?.length || 0} frames</small>
                      </summary>
                      <div className="cause-message">{exception.message || issue.title}</div>
                      {exception.stacktrace ? <pre className="payload">{exception.stacktrace}</pre> : <StackTrace frames={exception.frames} />}
                    </details>
                    ))}
                  </div>
                )}
              </details>
            ) : textualStacktrace ? (
              <details className="event-section" open>
                <summary className="event-section-head"><span>// STACK TRACE</span><small>OTLP TEXT</small></summary>
                <div className="stack-exception-head">
                  <div>
                    <strong>{String(fields["error.type"] || "Error")}</strong>
                    <p>{String(fields["error.value"] || issue.title)}</p>
                  </div>
                </div>
                <pre className="payload">{textualStacktrace}</pre>
              </details>
            ) : (
              <details className="event-section" open>
                <summary className="event-section-head"><span>// MESSAGE</span></summary>
                <div className="exception-title"><p>{fieldString(event.record, "message") || issue.title}</p></div>
              </details>
            )}

            {http.length > 0 && (
              <details className="event-section" open>
                <summary className="event-section-head"><span>// HTTP REQUEST</span></summary>
                <div className="request-line"><strong>{String(fields["http.method"] || "GET")}</strong><code>{String(fields["http.url"] || "unknown URL")}</code></div>
                <InfoCard title="REQUEST DATA" values={http.filter(([name]) => !["method", "url", "headers", "body", "data"].includes(name))} />
                <InfoCard title="HEADERS" values={pairs(fields["http.headers"])} />
                {requestBody != null && (
                  <details className="payload-block">
                    <summary>REQUEST BODY</summary>
                    <pre className="payload">{display(requestBody)}</pre>
                  </details>
                )}
              </details>
            )}

            {breadcrumbs.length > 0 && (
              <details className="event-section" open>
                <summary className="event-section-head"><span>// BREADCRUMBS [{breadcrumbs.length}]</span></summary>
                <div className="breadcrumbs">
                  {breadcrumbs.map((crumb, index) => (
                    <div key={index}>
                      <time>{displayTime(crumb.timestamp)}</time>
                      <span className={`level ${String(crumb.level || "info")}`}>{String(crumb.category || crumb.type || "event")}</span>
                      <p>{String(crumb.message || display(crumb.data) || "-")}</p>
                    </div>
                  ))}
                </div>
              </details>
            )}

            <details className="raw-event">
              <summary>RAW EVENT PAYLOAD</summary>
              <pre>{JSON.stringify(event.record.raw, null, 2)}</pre>
            </details>
          </div>

          <aside className="issue-context">
            <InfoCard title="EVENT" values={[
              ["record_id", event.record.id],
              ["source", event.record.source],
              ["source_id", event.record.source_id],
              ["timestamp", new Date(recordTimeMs(event.record)).toISOString()],
              ["platform", event.record.fields.platform],
              ["environment", event.record.fields.environment],
              ["release", event.record.fields.release],
              ["transaction", event.record.fields.transaction],
              ["server", event.record.fields.server],
            ]} />
            <InfoCard title="USER" values={prefixedPairs(fields, "user.")} />
            <InfoCard title="TAGS" values={prefixedPairs(fields, "tag.")} />
            <InfoCard title="TRACE" values={trace} />
            <InfoCard title="SDK" values={prefixedPairs(fields, "sdk.")} />
            <InfoCard title="CONTEXTS" values={prefixedPairs(fields, "context.")} />
            <InfoCard title="ATTRIBUTES" values={prefixedPairs(fields, "attribute.")} />
            <InfoCard title="RESOURCE" values={prefixedPairs(fields, "resource.")} />
            <InfoCard title="SCOPE" values={prefixedPairs(fields, "scope.")} />
          </aside>
        </div>
      ) : null}
    </div>
  );
}

function SettingsPage({ theme, onTheme }: { theme: Theme; onTheme: (theme: Theme) => void }) {
  return (
    <div className="settings-page">
      <header className="settings-head">
        <p className="eyebrow">root / settings</p>
        <h1>settings<span className="cursor">_</span></h1>
      </header>
      <section className="settings-section">
        <div className="settings-section-head">
          <span>// APPEARANCE</span>
          <small>palette::rosé_pine</small>
        </div>
        <fieldset>
          <legend>color theme</legend>
          {themeOptions.map((option) => (
            <label className={`theme-option ${theme === option.value ? "active" : ""}`} key={option.value}>
              <input
                type="radio"
                name="theme"
                value={option.value}
                checked={theme === option.value}
                onChange={() => onTheme(option.value)}
              />
              <span aria-hidden="true">{theme === option.value ? "[●]" : "[ ]"}</span>
              <strong>{option.label}</strong>
              <small>{option.detail}</small>
            </label>
          ))}
        </fieldset>
      </section>
    </div>
  );
}

function DocsPage() {
  const mcpUrl = `${location.origin}/mcp`;
  return (
    <div className="settings-page">
      <header className="settings-head">
        <p className="eyebrow">root / docs</p>
        <h1>connect MCP<span className="cursor">_</span></h1>
      </header>

      <section className="settings-section">
        <div className="settings-section-head"><span>// STREAMABLE HTTP</span><small>recommended</small></div>
        <div className="docs-content">
          <p>Use these values in an MCP client that supports remote servers:</p>
          <dl>
            <div><dt>transport</dt><dd>Streamable HTTP</dd></div>
            <div><dt>url</dt><dd><code>{mcpUrl}</code></dd></div>
          </dl>
        </div>
      </section>

      <section className="settings-section">
        <div className="settings-section-head"><span>// STDIO</span><small>local clients</small></div>
        <div className="docs-content">
          <p>Use the CLI bridge when your client starts local MCP processes:</p>
          <pre>{`{
  "mcpServers": {
    "ntry": {
      "command": "ntry",
      "args": ["mcp"]
    }
  }
}`}</pre>
          <p>The command reads <code>NTRY_URL</code>. Its default connects to the local daemon.</p>
        </div>
      </section>

      <section className="settings-section">
        <div className="settings-section-head"><span>// TOOLS</span><small>read only</small></div>
        <div className="docs-content">
          <p><code>search</code> issues, errors, logs, and traces; <code>get</code> one record; <code>query_metrics</code>; and check daemon <code>status</code>.</p>
          <p>Browser clients must use an Origin that matches the server Host.</p>
        </div>
      </section>
    </div>
  );
}

function App() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [projectId, setProjectId] = useState<number | null>(initialRoute.projectId);
  const [tab, setTab] = useState<Tab>(initialRoute.tab);
  const [range, setRange] = useState<keyof typeof ranges>(initialRoute.range);
  const [query, setQuery] = useState(initialRoute.query);
  const [queryDraft, setQueryDraft] = useState(initialRoute.query);
  const [routeIssueId, setRouteIssueId] = useState<string | null>(initialRoute.issueId);
  const [view, setView] = useState<View | null>(null);
  const [status, setStatus] = useState<Status | null>(null);
  const [selected, setSelected] = useState<Record | null>(null);
  const [traceRecords, setTraceRecords] = useState<Record[]>([]);
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceError, setTraceError] = useState("");
  const [selectedIssue, setSelectedIssue] = useState<Issue | null>(null);
  const [issueEvent, setIssueEvent] = useState<IssueEvent | null>(null);
  const [newProject, setNewProject] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [copied, setCopied] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);
  const [live, setLive] = useState(false);
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [staticPage, setStaticPage] = useState<StaticPage>(location.pathname === "/settings" ? "settings" : location.pathname === "/docs" ? "docs" : null);

  useEffect(() => {
    localStorage.setItem("ntry-theme", theme);
    applyTheme(theme);
    if (theme !== "system") return;
    const query = matchMedia("(prefers-color-scheme: dark)");
    const update = () => applyTheme("system");
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, [theme]);

  useEffect(() => {
    api<Project[]>("/api/v1/projects")
      .then((next) => {
        setProjects(next);
        setProjectId((current) => (next.some((project) => project.id === current) ? current : next[0]?.id ?? null));
      })
      .catch((reason) => setError(reason.message));
  }, []);

  useEffect(() => {
    const restore = () => {
      const page = location.pathname === "/settings" ? "settings" : location.pathname === "/docs" ? "docs" : null;
      setStaticPage(page);
      if (page) {
        setSelected(null);
        setSelectedIssue(null);
        setIssueEvent(null);
        return;
      }
      const route = readRoute();
      setProjectId(route.projectId);
      setTab(route.tab);
      setRange(route.range);
      setQuery(route.query);
      setQueryDraft(route.query);
      setRouteIssueId(route.issueId);
      setSelected(null);
      setSelectedIssue(null);
      setIssueEvent(null);
    };
    window.addEventListener("popstate", restore);
    return () => window.removeEventListener("popstate", restore);
  }, []);

  useEffect(() => {
    if (staticPage || projectId === null) return;
    const path = routePath(projectId, tab, routeIssueId, range, query);
    if (location.pathname + location.search !== path) history.replaceState(null, "", path);
  }, [projectId, query, range, routeIssueId, staticPage, tab]);

  useEffect(() => {
    if (!routeIssueId || projectId === null || selectedIssue?.id === routeIssueId) return;
    const controller = new AbortController();
    const params = new URLSearchParams({
      project_id: String(projectId),
      query: `issue:${routeIssueId}`,
      limit: "1",
      start_ms: "0",
      end_ms: String(Date.now()),
    });
    api<Page<Issue>>(`/api/v1/issues?${params}`, { signal: controller.signal })
      .then((page) => {
        if (page.items[0]) showIssue(page.items[0]);
        else setError(`issue ${routeIssueId} was not found`);
      })
      .catch((reason) => {
        if (reason.name !== "AbortError") setError(reason.message);
      });
    return () => controller.abort();
  }, [projectId, routeIssueId, selectedIssue?.id]);

  useEffect(() => {
    if (projectId === null) {
      setView(null);
      return;
    }
    let active = true;
    const controller = new AbortController();
    const end = Date.now();
    const params = new URLSearchParams({
      project_id: String(projectId),
      query,
      limit: "100",
      start_ms: String(end - ranges[range]),
      end_ms: String(end),
    });
    const path = tab === "issues" ? "/api/v1/issues" : `/api/v1/records/${tabSignals[tab]}`;
    setLoading(true);
    setError("");
    Promise.all([
      api<Page<Issue | Record>>(`${path}?${params}`, { signal: controller.signal }),
      api<Status>("/api/v1/status", { signal: controller.signal }),
    ])
      .then(([page, nextStatus]) => {
        if (!active) return;
        startTransition(() => {
          setView({ projectId, tab, range, query, items: page.items });
          setStatus(nextStatus);
          setSelected(null);
        });
      })
      .catch((reason) => {
        if (active && reason.name !== "AbortError") setError(reason.message);
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [projectId, query, range, refresh, tab]);

  useEffect(() => {
    if (projectId === null) return;
    let refreshTimer = 0;
    const events = new EventSource("/api/v1/live");
    events.onopen = () => setLive(true);
    events.onerror = () => setLive(false);
    events.onmessage = ({ data }) => {
      const change = JSON.parse(data) as Change;
      if (change.project_id === projectId && change.signal === tabSignals[tab]) {
        clearTimeout(refreshTimer);
        refreshTimer = window.setTimeout(() => setRefresh((value) => value + 1), 150);
      }
    };
    return () => {
      events.close();
      clearTimeout(refreshTimer);
      setLive(false);
    };
  }, [projectId, tab]);

  const selectedTraceId = selected?.signal === "trace" ? fieldString(selected, "trace_id") : null;
  useEffect(() => {
    setTraceRecords([]);
    setTraceError("");
    if (!selectedTraceId || projectId === null) {
      setTraceLoading(false);
      return;
    }
    const controller = new AbortController();
    const end = Date.now();
    setTraceLoading(true);
    loadAllPages<Record>(async (cursor) => {
        const params = new URLSearchParams({
          project_id: String(projectId),
          query: `trace_id:${queryLiteral(selectedTraceId)}`,
          limit: "500",
          start_ms: "0",
          end_ms: String(end),
        });
        if (cursor) params.set("cursor", cursor);
        return api<Page<Record>>(`/api/v1/records/trace?${params}`, { signal: controller.signal });
      })
      .then(setTraceRecords)
      .catch((reason) => {
        if (reason.name !== "AbortError") setTraceError(reason.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setTraceLoading(false);
      });
    return () => controller.abort();
  }, [projectId, selectedTraceId]);

  useEffect(() => {
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeDetail();
    };
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [projectId, query, range, routeIssueId, selectedIssue, tab]);

  const activeProject = projects.find((project) => project.id === projectId);
  const activeDsn = activeProject ? ingestUrl(status?.listeners.sentry, `/${activeProject.id}`, activeProject.key) : null;
  const activeDdtraceAgentUrl = activeProject ? ingestUrl(status?.listeners.ddtrace, `/${activeProject.id}/${activeProject.key}/`) : null;
  const activeOtlpEndpoint = activeProject ? ingestUrl(status?.listeners.otlp, `/${activeProject.id}/`) : null;
  const items = view?.projectId === projectId && view.tab === tab && view.range === range && view.query === query ? view.items : [];

  async function addProject(event: FormEvent) {
    event.preventDefault();
    const name = newProject.trim();
    if (!name) return;
    try {
      const project = await api<Project>("/api/v1/projects", { method: "POST", body: JSON.stringify({ name }) });
      setProjects((current) => [...current, project]);
      setProjectId(project.id);
      setRouteIssueId(null);
      history.pushState(null, "", routePath(project.id, tab, null, range, query));
      setNewProject("");
    } catch (reason) {
      setError((reason as Error).message);
    }
  }

  async function removeProject(project: Project) {
    if (!confirm(`Delete ${project.name} and all of its telemetry?`)) return;
    try {
      await api(`/api/v1/projects/${encodeURIComponent(project.name)}`, { method: "DELETE" });
      const next = projects.filter((item) => item.id !== project.id);
      setProjects(next);
      if (projectId === project.id) {
        const nextId = next[0]?.id ?? null;
        setProjectId(nextId);
        setRouteIssueId(null);
        setSelectedIssue(null);
        setIssueEvent(null);
        history.replaceState(null, "", nextId === null ? "/" : routePath(nextId, tab, null, range, query));
      }
    } catch (reason) {
      setError((reason as Error).message);
    }
  }

  async function copyIngest(value: string, key: string) {
    if (await copyText(value)) {
      setCopied(key);
      setTimeout(() => setCopied((current) => current === key ? null : current), 1_200);
    } else {
      setError("browser blocked clipboard access");
    }
  }

  function clearDetailState() {
    setSelected(null);
    setSelectedIssue(null);
    setIssueEvent(null);
    setRouteIssueId(null);
  }

  function closeDetail() {
    const issueWasOpen = selectedIssue !== null || routeIssueId !== null;
    clearDetailState();
    if (issueWasOpen && projectId !== null) history.pushState(null, "", routePath(projectId, tab, null, range, query));
  }

  function showIssue(issue: Issue) {
    setSelected(null);
    setSelectedIssue(issue);
    setIssueEvent(issue.retained_events ? { issueId: issue.id } : null);
    if (!issue.retained_events) return;
    const params = new URLSearchParams({
      project_id: String(issue.project_id),
      query: `issue:${issue.id}`,
      limit: "500",
      start_ms: String(issue.first_seen_ms),
      end_ms: String(issue.last_seen_ms),
    });
    api<Page<Record>>(`/api/v1/records/error?${params}`)
      .then(({ items }) => setIssueEvent((current) => current?.issueId === issue.id ? {
        issueId: issue.id,
        records: items,
        record: items.find((record) => record.id === issue.latest_record_id) || items[0],
      } : current))
      .catch((reason) => setIssueEvent((current) => current?.issueId === issue.id ? { issueId: issue.id, error: reason.message } : current));
  }

  function openIssue(issue: Issue) {
    setRouteIssueId(issue.id);
    history.pushState(null, "", routePath(issue.project_id, "issues", issue.id, range, query));
    showIssue(issue);
  }

  function selectProject(id: number) {
    clearDetailState();
    setStaticPage(null);
    setProjectId(id);
    history.pushState(null, "", routePath(id, tab, null, range, query));
  }

  function openStaticPage(page: Exclude<StaticPage, null>) {
    clearDetailState();
    setStaticPage(page);
    history.pushState(null, "", `/${page}`);
  }

  function selectTab(next: Tab) {
    if (projectId !== null) history.pushState(null, "", routePath(projectId, next, null, range, query));
    clearDetailState();
    setTab(next);
  }

  function addToQuery(section: string, name: string, value: string | number | boolean) {
    const field = section === "ATTRIBUTES" ? `attribute.${name}` : name === "record_id" ? "id" : name;
    const clause = `${field}:${queryLiteral(value)}`;
    const next = query ? `${query} AND ${clause}` : clause;
    setQueryDraft(next);
    setQuery(next);
    setSelected(null);
  }

  const metrics = tab === "metrics"
    ? Object.values(
        (items as Record[]).reduce<globalThis.Record<string, Record[]>>((groups, record) => {
          const key = `${metricName(record)}:${metricType(record)}:${metricUnit(record)}`;
          (groups[key] ||= []).push(record);
          return groups;
        }, {}),
      )
    : [];

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark">[</span> N T R Y <span className="brand-mark">]</span>
          <small>sentry at home</small>
        </div>

        <section className="project-block">
          <div className="section-label">// PROJECTS [{projects.length}]</div>
          <div className="project-list">
            {projects.map((project) => (
              <div className="project-row-wrap" key={project.id}>
                <button
                  className={`project-row ${project.id === projectId ? "active" : ""}`}
                  onClick={() => selectProject(project.id)}
                >
                  <span className="truncate">{project.id === projectId ? ">" : " "} {project.name}</span>
                  <span className="status-dot" title={project.status} />
                </button>
                {project.id === projectId && <button className="project-delete" aria-label={`Delete ${project.name}`} title={`Delete ${project.name}`} onClick={() => removeProject(project)}>DEL</button>}
              </div>
            ))}
            {!projects.length && <p className="empty-small">no projects mounted</p>}
          </div>
          <form className="add-project" onSubmit={addProject}>
            <label className="sr-only" htmlFor="project-name">New project name</label>
            <input id="project-name" value={newProject} onChange={(event) => setNewProject(event.target.value)} placeholder="project_name" />
            <button title="Add project">+ ADD</button>
          </form>
        </section>

        {activeProject && (
          <div className="ingest-blocks">
            <section className="dsn-block">
              <div className="section-label">// SENTRY DSN</div>
              <button
                className="code-copy"
                disabled={!activeDsn}
                aria-label={copied === `sentry-${activeProject.id}` ? "DSN copied" : "Copy Sentry DSN"}
                title={copied === `sentry-${activeProject.id}` ? "Copied" : "Copy Sentry DSN"}
                onClick={() => activeDsn && copyIngest(activeDsn, `sentry-${activeProject.id}`)}
              >
                <code>{activeDsn || "SENTRY DISABLED"}</code>
                {activeDsn && (copied === `sentry-${activeProject.id}` ? "OK" : "CP")}
              </button>
            </section>
            <section className="dsn-block">
              <div className="section-label">// DDTRACE AGENT URL</div>
              <button
                className="code-copy"
                disabled={!activeDdtraceAgentUrl}
                aria-label={copied === `ddtrace-${activeProject.id}` ? "DDTrace agent URL copied" : "Copy DDTrace agent URL"}
                title={copied === `ddtrace-${activeProject.id}` ? "Copied" : "Copy DDTrace agent URL"}
                onClick={() => activeDdtraceAgentUrl && copyIngest(activeDdtraceAgentUrl, `ddtrace-${activeProject.id}`)}
              >
                <code>{activeDdtraceAgentUrl || "DDTRACE DISABLED"}</code>
                {activeDdtraceAgentUrl && (copied === `ddtrace-${activeProject.id}` ? "OK" : "CP")}
              </button>
            </section>
            <section className="dsn-block">
              <div className="section-label">// OTLP HTTP</div>
              {activeOtlpEndpoint ? <>
                <button
                  className="code-copy"
                  aria-label={copied === `otlp-endpoint-${activeProject.id}` ? "OTLP endpoint copied" : "Copy OTLP endpoint"}
                  title={copied === `otlp-endpoint-${activeProject.id}` ? "Copied" : "Copy OTLP endpoint"}
                  onClick={() => copyIngest(activeOtlpEndpoint, `otlp-endpoint-${activeProject.id}`)}
                >
                  <code>{activeOtlpEndpoint}</code>
                  {copied === `otlp-endpoint-${activeProject.id}` ? "OK" : "CP"}
                </button>
                <button
                  className="code-copy"
                  aria-label={copied === `otlp-auth-${activeProject.id}` ? "OTLP authorization copied" : "Copy OTLP authorization header"}
                  title={copied === `otlp-auth-${activeProject.id}` ? "Copied" : "Copy OTLP authorization header"}
                  onClick={() => copyIngest(`Authorization: Bearer ${activeProject.key}`, `otlp-auth-${activeProject.id}`)}
                >
                  <code>Authorization: Bearer {activeProject.key}</code>
                  {copied === `otlp-auth-${activeProject.id}` ? "OK" : "CP"}
                </button>
              </> : <code>OTLP DISABLED</code>}
            </section>
          </div>
        )}

        <button className={`settings-link ${staticPage === "settings" ? "active" : ""}`} onClick={() => openStaticPage("settings")}>
          <span>{staticPage === "settings" ? ">" : " "} settings</span><span>[::]</span>
        </button>
        <button className={`settings-link ${staticPage === "docs" ? "active" : ""}`} onClick={() => openStaticPage("docs")}>
          <span>{staticPage === "docs" ? ">" : " "} docs</span><span>[?]</span>
        </button>

        <div className="sidebar-foot">
          <span><i className="status-dot" /> ONLINE</span>
          <span>localhost::{status?.listeners.web_api || location.port || "80"}</span>
        </div>
      </aside>

      <main className={`main-panel ${selectedIssue ? "issue-page-panel" : ""}`}>
        {staticPage === "settings" ? (
          <SettingsPage theme={theme} onTheme={setTheme} />
        ) : staticPage === "docs" ? (
          <DocsPage />
        ) : selectedIssue ? (
          <IssuePage
            issue={selectedIssue}
            event={issueEvent?.issueId === selectedIssue.id ? issueEvent : null}
            onSelectEvent={(id) => setIssueEvent((current) => current?.issueId === selectedIssue.id ? {
              ...current,
              record: current.records?.find((record) => record.id === id) || current.record,
            } : current)}
            onBack={closeDetail}
          />
        ) : <>
        <header className="topbar">
          <div>
            <p className="eyebrow">root / projects / {activeProject?.name || "null"}</p>
            <h1>{activeProject?.name || "NO PROJECT"}<span className="cursor">_</span></h1>
          </div>
        </header>

        <section className="stat-strip">
          <div><span>EVENTS::TOTAL</span><strong>{status?.records.toLocaleString() ?? "--"}</strong></div>
          <div><span>INGEST::ACCEPTED</span><strong>{status?.process.accepted.toLocaleString() ?? "--"}</strong></div>
           <div><span>DISK::USED</span><strong className={status?.storage_warning ? "warning-text" : ""}>{status ? bytes(status.disk_bytes) : "--"}</strong></div>
          <div><span>ERRORS::PROCESS</span><strong>{status ? status.process.failed + status.process.malformed : "--"}</strong></div>
        </section>

        <div className="toolbar">
          <nav aria-label="Telemetry view">
            {tabs.map((name) => (
              <button key={name} className={tab === name ? "active" : ""} onClick={() => selectTab(name)}>
                {tab === name ? `[ ${name.toUpperCase()} ]` : name.toUpperCase()}
              </button>
            ))}
          </nav>
          <div className="range-picker">
            {(Object.keys(ranges) as (keyof typeof ranges)[]).map((name) => (
              <button key={name} className={range === name ? "active" : ""} onClick={() => setRange(name)}>{name}</button>
            ))}
            <button aria-label="Refresh" onClick={() => setRefresh((value) => value + 1)}>R</button>
          </div>
        </div>

        <form className="query-bar" onSubmit={(event) => { event.preventDefault(); setQuery(queryDraft); }}>
          <span>ntry@local:~$</span>
          <label className="sr-only" htmlFor="query">Search telemetry</label>
          <input id="query" value={queryDraft} onChange={(event) => setQueryDraft(event.target.value)} placeholder="level:error AND message:*timeout" />
          <button>RUN_QUERY</button>
        </form>

        {error && <div className="error-banner"><span>! ERR</span> {error}<button onClick={() => setError("")}>[x]</button></div>}

        <section className="data-panel">
          <div className="panel-head">
            <span>// {tab.toUpperCase()}_STREAM</span>
            <span>{live ? "LIVE :: " : ""}{loading ? "SCANNING..." : `${tab === "metrics" ? metrics.length : items.length} ROWS`}</span>
          </div>

          {!activeProject ? (
            <div className="empty-state"><pre>[ NO_PROJECT ]</pre><p>Add a project from the left rail.</p></div>
          ) : !loading && !items.length ? (
            <div className="empty-state"><pre>{`> scan --range ${range}\n> 0 signals found_`}</pre><p>Waiting for telemetry or try a wider range.</p></div>
          ) : tab === "issues" ? (
            <div>
              {(items as Issue[]).map((issue) => (
                <button className="issue-row" key={issue.id} onClick={() => openIssue(issue)}>
                  <span className={`level ${issue.level}`}>{issue.level.toUpperCase()}</span>
                  <div className="min-w-0">
                    <h2>{issue.title}</h2>
                    <p>{issue.id.slice(0, 12)} :: first {ago(issue.first_seen_ms)} :: {issue.retained_events} retained</p>
                  </div>
                  <div className="issue-count"><strong>{issue.total_seen}</strong><span>events</span></div>
                  <time>{ago(issue.last_seen_ms)}</time>
                </button>
              ))}
            </div>
          ) : tab === "logs" ? (
            <div>
              {(items as Record[]).map((record) => (
                <button className="log-row" key={record.id} onClick={() => { setSelectedIssue(null); setSelected(record); }}>
                  <time>{new Date(recordTimeMs(record)).toLocaleTimeString([], { hour12: false })}</time>
                  <span className={`level ${level(record)}`}>{level(record).toUpperCase()}</span>
                  <span className="log-message">{text(record)}</span>
                  <span className="log-source">{fieldString(record, "logger") || fieldString(record, "resource.service.name") || record.source}</span>
                  <span className="row-arrow">-&gt;</span>
                </button>
              ))}
            </div>
          ) : tab === "metrics" ? (
            <div className="metric-grid">
              {metrics.map((records) => (
                <button className="metric-card" key={`${metricName(records[0])}:${metricType(records[0])}`} onClick={() => { setSelectedIssue(null); setSelected(records[0]); }}>
                  <div className="metric-title"><span>{metricName(records[0])}</span><small>{metricType(records[0])}</small></div>
                  <Bars records={records} />
                  <div className="metric-foot"><strong>{metricValue(records[0]).toLocaleString()}</strong><span>{metricUnit(records[0])} / {records.length} samples</span></div>
                </button>
              ))}
            </div>
          ) : (
            <div>
              {(items as Record[]).map((record) => (
                <button className="trace-row" key={record.id} onClick={() => { setSelectedIssue(null); setSelected(record); }}>
                  <time>{new Date(recordTimeMs(record)).toLocaleTimeString([], { hour12: false })}</time>
                  <span className={`level ${traceStatus(record)}`}>{traceStatus(record).toUpperCase()}</span>
                  <span className="trace-name">{fieldString(record, "name") || "unnamed span"}</span>
                  <span className="trace-service">{fieldString(record, "resource.service.name") || record.source}</span>
                  <span className="trace-duration">{traceDuration(record)}</span>
                  <span className="row-arrow">-&gt;</span>
                </button>
              ))}
            </div>
          )}
        </section>
        </>}
      </main>

      {selected && <RecordDetail
        record={selected}
        traceRecords={traceRecords}
        traceLoading={traceLoading}
        traceError={traceError}
        onSelectTrace={setSelected}
        onAddQuery={addToQuery}
        onClose={closeDetail}
      />}
    </div>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
