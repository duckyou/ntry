use anyhow::{Context, Result, bail};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{
    MetricPoint, MetricQueryRequest, MetricQueryResponse, MetricSeries, SearchCursor, StoredRecord,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Predicate(Predicate),
    Text(String),
    All,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Predicate {
    field: String,
    operator: Operator,
    values: Vec<Literal>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Operator {
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Exists,
}

#[derive(Clone, Debug, PartialEq)]
enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Atom(String),
    And,
    Or,
    Not,
    LeftParen,
    RightParen,
}

pub fn parse(input: &str) -> Result<Expr> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Ok(Expr::All);
    }
    let mut parser = Parser { tokens, cursor: 0 };
    let expression = parser.parse_or()?;
    if parser.cursor != parser.tokens.len() {
        bail!("unexpected token in query");
    }
    Ok(expression)
}

pub fn encode_cursor(cursor: &SearchCursor) -> String {
    serde_json::to_string(cursor).expect("search cursors serialize")
}

pub fn decode_cursor(cursor: Option<&str>) -> Result<Option<SearchCursor>> {
    cursor
        .map(|cursor| {
            if cursor.len() > 1_024 {
                bail!("cursor is too large");
            }
            serde_json::from_str(cursor).context("invalid cursor")
        })
        .transpose()
}

#[derive(Clone, Debug)]
pub enum IndexHint {
    Level(String),
    Issue(String),
    Trace(String),
}

pub fn index_hint(expression: &Expr) -> Option<IndexHint> {
    match expression {
        Expr::And(expressions) => expressions.iter().find_map(index_hint),
        Expr::Predicate(predicate)
            if predicate.operator == Operator::Equal && predicate.values.len() == 1 =>
        {
            let Literal::String(value) = &predicate.values[0] else {
                return None;
            };
            if value.contains('*') {
                return None;
            }
            match predicate.field.as_str() {
                "level" => Some(IndexHint::Level(value.clone())),
                "issue" => Some(IndexHint::Issue(value.clone())),
                "trace" | "trace_id" => Some(IndexHint::Trace(value.clone())),
                _ => None,
            }
        }
        _ => None,
    }
}

pub fn validate_fields(expression: &Expr) -> Result<()> {
    match expression {
        Expr::And(expressions) | Expr::Or(expressions) => {
            for expression in expressions {
                validate_fields(expression)?;
            }
        }
        Expr::Not(expression) => validate_fields(expression)?,
        Expr::Predicate(predicate) => validate_field(&predicate.field)?,
        Expr::Text(_) | Expr::All => {}
    }
    Ok(())
}

pub fn validate_field(field: &str) -> Result<()> {
    const FIELDS: &[&str] = &[
        "id",
        "event_id",
        "source",
        "source_id",
        "signal",
        "issue",
        "project.id",
        "timestamp",
        "event.timestamp",
        "received",
        "received_at",
        "message",
        "title",
        "level",
        "logger",
        "transaction",
        "platform",
        "environment",
        "release",
        "dist",
        "server",
        "server_name",
        "trace",
        "trace_id",
        "span_id",
        "parent_span_id",
        "trace.op",
        "trace.status",
        "sdk.name",
        "sdk.version",
        "user.id",
        "user.email",
        "user.username",
        "user.ip",
        "http.method",
        "http.url",
        "error.type",
        "error.value",
        "error.handled",
        "stack.filename",
        "stack.function",
        "stack.module",
        "name",
        "kind",
        "type",
        "unit",
        "value",
        "firstSeen",
        "lastSeen",
        "timesSeen",
    ];
    if FIELDS.contains(&field)
        || field
            .strip_prefix("tags[")
            .and_then(|field| field.strip_suffix(']'))
            .is_some_and(|field| !field.is_empty())
    {
        return Ok(());
    }
    if [
        "project.", "event.", "error.", "stack.", "http.", "user.", "sdk.", "trace.",
    ]
    .iter()
    .any(|prefix| field.starts_with(prefix))
    {
        bail!("unsupported field {field:?}");
    }
    Ok(())
}

pub fn matches(record: &StoredRecord, expression: &Expr) -> bool {
    match expression {
        Expr::All => true,
        Expr::And(expressions) => expressions.iter().all(|expr| matches(record, expr)),
        Expr::Or(expressions) => expressions.iter().any(|expr| matches(record, expr)),
        Expr::Not(expression) => !matches(record, expression),
        Expr::Text(text) => searchable_text(record)
            .iter()
            .any(|value| value.to_lowercase().contains(&text.to_lowercase())),
        Expr::Predicate(predicate) => {
            let values = field_values(record, &predicate.field);
            if predicate.operator == Operator::Exists {
                return !values.is_empty();
            }
            values.iter().any(|stored| {
                predicate
                    .values
                    .iter()
                    .any(|expected| compare(stored, expected, predicate.operator))
            })
        }
    }
}

pub fn event_time_ms(record: &StoredRecord) -> u64 {
    record.timestamp_unix_nano / 1_000_000
}

pub fn parse_time_ms(value: &str) -> Result<u64> {
    if let Ok(number) = value.parse::<f64>()
        && number.is_finite()
        && number >= 0.0
    {
        return Ok((number * 1_000.0) as u64);
    }
    let time = OffsetDateTime::parse(value, &Rfc3339).context("timestamp must be RFC 3339")?;
    u64::try_from(time.unix_timestamp_nanos() / 1_000_000).context("timestamp is before 1970")
}

pub fn parse_duration_ms(value: &str) -> Result<u64> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .context("duration needs a unit: s, m, h, d, or w")?;
    let amount = value[..split].parse::<u64>()?;
    let multiplier = match &value[split..] {
        "s" => 1_000,
        "m" => 60 * 1_000,
        "h" => 60 * 60 * 1_000,
        "d" => 24 * 60 * 60 * 1_000,
        "w" => 7 * 24 * 60 * 60 * 1_000,
        _ => bail!("duration unit must be s, m, h, d, or w"),
    };
    amount
        .checked_mul(multiplier)
        .context("duration is too large")
}

pub fn field_values(record: &StoredRecord, field: &str) -> Vec<Value> {
    let canonical = match field {
        "event_id" => "source_id",
        "event.timestamp" => "timestamp",
        "received_at" => "received",
        "server_name" => "server",
        "trace" => "trace_id",
        "kind" => "type",
        _ => field,
    };
    if let Some(value) = normalized_fields(record).remove(canonical) {
        return values(value);
    }

    if let Some(key) = field
        .strip_prefix("tags[")
        .and_then(|key| key.strip_suffix(']'))
    {
        return record
            .fields
            .get(&format!("attribute.{key}"))
            .cloned()
            .map(values)
            .unwrap_or_default();
    }
    record
        .fields
        .get(&format!("attribute.{field}"))
        .cloned()
        .map(values)
        .unwrap_or_default()
}

pub fn normalized_fields(record: &StoredRecord) -> BTreeMap<String, Value> {
    let mut fields = record.fields.clone();
    fields.insert("id".into(), Value::String(record.id.clone()));
    fields.insert("project.id".into(), Value::Number(record.project_id.into()));
    fields.insert("source".into(), Value::String(record.source.clone()));
    fields.insert(
        "signal".into(),
        Value::String(record.signal.as_str().to_owned()),
    );
    fields.insert(
        "received".into(),
        Value::Number(record.received_at_ms.into()),
    );
    insert(
        &mut fields,
        "source_id",
        record.source_id.clone().map(Value::String),
    );
    insert(
        &mut fields,
        "issue",
        record.issue_id.clone().map(Value::String),
    );
    fields.insert(
        "timestamp".into(),
        Value::Number(record.timestamp_unix_nano.into()),
    );
    fields
}

fn insert(fields: &mut BTreeMap<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value);
    }
}

fn values(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        value => vec![value],
    }
}

pub fn aggregate_metrics(
    records: impl IntoIterator<Item = StoredRecord>,
    request: &MetricQueryRequest,
    expression: &Expr,
) -> Result<MetricQueryResponse> {
    if request.name.is_empty() {
        bail!("metric name cannot be empty");
    }
    if request.group_by.len() > 3 {
        bail!("metric queries allow at most three group fields");
    }
    if request.metric_type.as_deref().is_some_and(|kind| {
        !matches!(
            kind,
            "sum" | "gauge" | "distribution" | "histogram" | "exponential_histogram" | "summary"
        )
    }) {
        bail!(
            "metric type must be sum, gauge, distribution, histogram, exponential_histogram, or summary"
        );
    }
    let group_limit = request.group_limit.unwrap_or(20);
    if !(1..=100).contains(&group_limit) {
        bail!("group limit must be between 1 and 100");
    }
    let start_ms = request
        .start_ms
        .context("metric query is missing start_ms")?;
    let end_ms = request.end_ms.context("metric query is missing end_ms")?;
    if start_ms > end_ms {
        bail!("query start must not be after its end");
    }

    let mut identities = BTreeSet::new();
    let mut samples = Vec::new();
    for record in records {
        let timestamp_ms = event_time_ms(&record);
        if timestamp_ms < start_ms
            || timestamp_ms > end_ms
            || record.fields.get("name").and_then(Value::as_str) != Some(&request.name)
            || !matches(&record, expression)
        {
            continue;
        }
        let metric_type = record
            .fields
            .get("type")
            .and_then(Value::as_str)
            .context("stored metric is missing type")?;
        let unit = record.fields.get("unit").and_then(Value::as_str);
        if request
            .metric_type
            .as_deref()
            .is_some_and(|kind| kind != metric_type)
        {
            continue;
        }
        if let Some(expected) = request.unit.as_deref()
            && unit != (!expected.is_empty()).then_some(expected)
        {
            continue;
        }
        identities.insert((metric_type.to_owned(), unit.map(str::to_owned)));
        let Some(value) = record.fields.get("value").and_then(Value::as_f64) else {
            if matches!(
                metric_type,
                "sum" | "histogram" | "exponential_histogram" | "summary"
            ) {
                continue;
            }
            bail!("stored metric is missing value");
        };
        let group = request
            .group_by
            .iter()
            .map(|field| {
                (
                    field.clone(),
                    field_values(&record, field)
                        .into_iter()
                        .next()
                        .unwrap_or(Value::Null),
                )
            })
            .collect::<BTreeMap<_, _>>();
        samples.push((timestamp_ms, value, group));
    }

    let (metric_type, unit) = match identities.len() {
        0 => bail!(
            "metric {:?} was not found in the selected range",
            request.name
        ),
        1 => identities.pop_first().unwrap(),
        _ => bail!("metric name is ambiguous; specify type and unit"),
    };
    if matches!(
        metric_type.as_str(),
        "histogram" | "exponential_histogram" | "summary"
    ) {
        bail!("aggregation is not supported for {metric_type}");
    }
    validate_aggregate(&metric_type, &request.aggregate)?;
    let interval_ms = request
        .interval_ms
        .filter(|interval| *interval > 0)
        .unwrap_or_else(|| automatic_interval(end_ms.saturating_sub(start_ms)));
    let mut grouped =
        BTreeMap::<String, (BTreeMap<String, Value>, BTreeMap<u64, Vec<Sample>>)>::new();
    for (timestamp_ms, value, group) in samples {
        let group_key = serde_json::to_string(&group)?;
        let bucket = start_ms + (timestamp_ms - start_ms) / interval_ms * interval_ms;
        grouped
            .entry(group_key)
            .or_insert_with(|| (group, BTreeMap::new()))
            .1
            .entry(bucket)
            .or_default()
            .push(Sample {
                timestamp_ms,
                value,
            });
    }

    let mut ranked = grouped
        .into_values()
        .map(|(group, buckets)| {
            let all = buckets.values().flatten().cloned().collect::<Vec<_>>();
            let rank = aggregate(&all, &metric_type, &request.aggregate, end_ms - start_ms)?;
            let points = buckets
                .into_iter()
                .map(|(timestamp_ms, values)| {
                    Ok(MetricPoint {
                        timestamp_ms,
                        value: aggregate(&values, &metric_type, &request.aggregate, interval_ms)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((rank, MetricSeries { group, points }))
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| right.0.total_cmp(&left.0));
    let series = ranked
        .into_iter()
        .take(group_limit)
        .map(|(_, series)| series)
        .collect::<Vec<_>>();
    if series
        .iter()
        .map(|series| series.points.len())
        .sum::<usize>()
        > 100_000
    {
        bail!("metric query exceeds the 100,000 point limit");
    }
    Ok(MetricQueryResponse {
        name: request.name.clone(),
        metric_type,
        unit,
        aggregate: request.aggregate.clone(),
        interval_ms,
        series,
    })
}

#[derive(Clone)]
struct Sample {
    timestamp_ms: u64,
    value: f64,
}

fn validate_aggregate(metric_type: &str, aggregate: &str) -> Result<()> {
    let valid = match metric_type {
        "sum" => matches!(
            aggregate,
            "increase"
                | "rate_per_second"
                | "rate_per_minute"
                | "sum"
                | "per_second"
                | "per_minute"
        ),
        "gauge" => matches!(
            aggregate,
            "min" | "max" | "avg" | "sum" | "count" | "latest"
        ),
        "distribution" => matches!(
            aggregate,
            "min"
                | "max"
                | "avg"
                | "sum"
                | "count"
                | "p50"
                | "p75"
                | "p90"
                | "p95"
                | "p99"
                | "per_second"
                | "per_minute"
        ),
        _ => false,
    };
    if !valid {
        bail!("aggregate {aggregate:?} is invalid for {metric_type} metrics");
    }
    Ok(())
}

fn aggregate(
    samples: &[Sample],
    metric_type: &str,
    aggregate: &str,
    period_ms: u64,
) -> Result<f64> {
    if samples.is_empty() {
        bail!("cannot aggregate an empty metric bucket");
    }
    let sum = samples.iter().map(|sample| sample.value).sum::<f64>();
    let value = match aggregate {
        "increase" | "sum" => sum,
        "count" => samples.len() as f64,
        "avg" => sum / samples.len() as f64,
        "min" => samples
            .iter()
            .map(|sample| sample.value)
            .fold(f64::INFINITY, f64::min),
        "max" => samples
            .iter()
            .map(|sample| sample.value)
            .fold(f64::NEG_INFINITY, f64::max),
        "latest" => {
            samples
                .iter()
                .max_by_key(|sample| sample.timestamp_ms)
                .unwrap()
                .value
        }
        "rate_per_second" | "rate_per_minute" | "per_second" | "per_minute" => {
            let base = if metric_type == "sum" {
                sum
            } else {
                samples.len() as f64
            };
            let units = if matches!(aggregate, "rate_per_second" | "per_second") {
                1_000.0
            } else {
                60_000.0
            };
            base * units / period_ms.max(1) as f64
        }
        percentile if percentile.starts_with('p') => {
            let percentile = percentile[1..].parse::<f64>()? / 100.0;
            let mut values = samples
                .iter()
                .map(|sample| sample.value)
                .collect::<Vec<_>>();
            values.sort_by(f64::total_cmp);
            let position = (values.len() - 1) as f64 * percentile;
            let lower = position.floor() as usize;
            let upper = position.ceil() as usize;
            values[lower] + (values[upper] - values[lower]) * position.fract()
        }
        _ => bail!("unsupported aggregate {aggregate:?}"),
    };
    Ok(value)
}

fn automatic_interval(range_ms: u64) -> u64 {
    const INTERVALS: &[u64] = &[
        1_000,
        5_000,
        10_000,
        30_000,
        60_000,
        5 * 60_000,
        15 * 60_000,
        60 * 60_000,
        6 * 60 * 60_000,
        24 * 60 * 60_000,
        7 * 24 * 60 * 60_000,
    ];
    let target = range_ms.div_ceil(299).max(1);
    INTERVALS
        .iter()
        .copied()
        .find(|interval| *interval >= target)
        .unwrap_or(30 * 24 * 60 * 60_000)
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let chars = input.char_indices().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let (_, character) = chars[index];
        if character.is_whitespace() {
            index += 1;
            continue;
        }
        match character {
            '(' => {
                tokens.push(Token::LeftParen);
                index += 1;
            }
            ')' => {
                tokens.push(Token::RightParen);
                index += 1;
            }
            '!' => {
                tokens.push(Token::Not);
                index += 1;
            }
            _ => {
                let start = chars[index].0;
                let mut quoted = character == '"';
                let mut escaped = false;
                let mut bracket_depth = 0;
                index += 1;
                while index < chars.len() {
                    let (_, current) = chars[index];
                    if escaped {
                        escaped = false;
                    } else if current == '\\' && quoted {
                        escaped = true;
                    } else if current == '"' {
                        quoted = !quoted;
                    } else if !quoted {
                        match current {
                            '[' => bracket_depth += 1,
                            ']' if bracket_depth > 0 => bracket_depth -= 1,
                            '(' | ')' if bracket_depth == 0 => break,
                            value if value.is_whitespace() && bracket_depth == 0 => break,
                            _ => {}
                        }
                    }
                    index += 1;
                }
                if quoted || bracket_depth != 0 {
                    bail!("unterminated quote or list in query");
                }
                let end = chars
                    .get(index)
                    .map_or(input.len(), |(position, _)| *position);
                let atom = &input[start..end];
                tokens.push(match atom {
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    _ => Token::Atom(atom.to_owned()),
                });
            }
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl Parser {
    fn parse_or(&mut self) -> Result<Expr> {
        let mut expressions = vec![self.parse_and()?];
        while self.peek() == Some(&Token::Or) {
            self.cursor += 1;
            expressions.push(self.parse_and()?);
        }
        Ok(if expressions.len() == 1 {
            expressions.pop().unwrap()
        } else {
            Expr::Or(expressions)
        })
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut expressions = vec![self.parse_unary()?];
        loop {
            let explicit = if self.peek() == Some(&Token::And) {
                self.cursor += 1;
                true
            } else {
                false
            };
            if self.cursor >= self.tokens.len()
                || matches!(self.peek(), Some(Token::Or | Token::RightParen))
            {
                if explicit {
                    bail!("query cannot end with AND");
                }
                break;
            }
            let next = self.parse_unary()?;
            if !explicit
                && let (Some(Expr::Text(left)), Expr::Text(right)) = (expressions.last_mut(), &next)
            {
                left.push(' ');
                left.push_str(right);
            } else {
                expressions.push(next);
            }
        }
        Ok(if expressions.len() == 1 {
            expressions.pop().unwrap()
        } else {
            Expr::And(expressions)
        })
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek() == Some(&Token::Not) {
            self.cursor += 1;
            return Ok(Expr::Not(Box::new(self.parse_unary()?)));
        }
        match self.next().context("query is incomplete")? {
            Token::LeftParen => {
                let expression = self.parse_or()?;
                if self.next() != Some(Token::RightParen) {
                    bail!("missing closing parenthesis");
                }
                Ok(expression)
            }
            Token::Atom(atom) => parse_atom(&atom),
            _ => bail!("unexpected query operator"),
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.cursor).cloned();
        self.cursor += usize::from(token.is_some());
        token
    }
}

fn parse_atom(atom: &str) -> Result<Expr> {
    let Some((field, raw_value)) = atom.split_once(':') else {
        return Ok(Expr::Text(unquote(atom)?));
    };
    if field.is_empty() || raw_value.is_empty() {
        bail!("field search requires both a field and value");
    }
    if field == "has" {
        return Ok(Expr::Predicate(Predicate {
            field: unquote(raw_value)?,
            operator: Operator::Exists,
            values: Vec::new(),
        }));
    }
    let (operator, raw_value) = if let Some(value) = raw_value.strip_prefix(">=") {
        (Operator::GreaterEqual, value)
    } else if let Some(value) = raw_value.strip_prefix("<=") {
        (Operator::LessEqual, value)
    } else if let Some(value) = raw_value.strip_prefix('>') {
        (Operator::Greater, value)
    } else if let Some(value) = raw_value.strip_prefix('<') {
        (Operator::Less, value)
    } else {
        (Operator::Equal, raw_value)
    };
    let values = if raw_value.starts_with('[') && raw_value.ends_with(']') {
        raw_value[1..raw_value.len() - 1]
            .split(',')
            .map(|value| parse_literal(value.trim()))
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![parse_literal(raw_value)?]
    };
    if values.is_empty() {
        bail!("field value list cannot be empty");
    }
    Ok(Expr::Predicate(Predicate {
        field: field.to_owned(),
        operator,
        values,
    }))
}

fn parse_literal(value: &str) -> Result<Literal> {
    if value.starts_with('"') {
        return Ok(Literal::String(unquote(value)?));
    }
    if value == "true" {
        return Ok(Literal::Bool(true));
    }
    if value == "false" {
        return Ok(Literal::Bool(false));
    }
    if let Ok(number) = value.parse::<f64>() {
        return Ok(Literal::Number(number));
    }
    Ok(Literal::String(value.to_owned()))
}

fn unquote(value: &str) -> Result<String> {
    if !value.starts_with('"') {
        return Ok(value.to_owned());
    }
    if !value.ends_with('"') || value.len() < 2 {
        bail!("unterminated quoted value");
    }
    let mut output = String::new();
    let mut escaped = false;
    for character in value[1..value.len() - 1].chars() {
        if escaped {
            output.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else {
            output.push(character);
        }
    }
    if escaped {
        bail!("unterminated escape in quoted value");
    }
    Ok(output)
}

fn compare(stored: &Value, expected: &Literal, operator: Operator) -> bool {
    match (stored, expected) {
        (Value::String(stored), Literal::String(expected)) => match operator {
            Operator::Equal => wildcard(stored, expected),
            Operator::Greater => stored > expected,
            Operator::GreaterEqual => stored >= expected,
            Operator::Less => stored < expected,
            Operator::LessEqual => stored <= expected,
            Operator::Exists => true,
        },
        (Value::Number(stored), Literal::Number(expected)) => {
            stored.as_f64().is_some_and(|value| match operator {
                Operator::Equal => value == *expected,
                Operator::Greater => value > *expected,
                Operator::GreaterEqual => value >= *expected,
                Operator::Less => value < *expected,
                Operator::LessEqual => value <= *expected,
                Operator::Exists => true,
            })
        }
        (Value::Bool(stored), Literal::Bool(expected)) => {
            operator == Operator::Equal && stored == expected
        }
        _ => false,
    }
}

fn wildcard(value: &str, pattern: &str) -> bool {
    let value = value.as_bytes();
    let pattern = pattern.as_bytes();
    let (mut value_index, mut pattern_index, mut star, mut checkpoint) = (0, 0, None, 0);
    while value_index < value.len() {
        if pattern.get(pattern_index) == value.get(value_index) {
            value_index += 1;
            pattern_index += 1;
        } else if pattern.get(pattern_index) == Some(&b'*') {
            star = Some(pattern_index);
            pattern_index += 1;
            checkpoint = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            checkpoint += 1;
            value_index = checkpoint;
        } else {
            return false;
        }
    }
    while pattern.get(pattern_index) == Some(&b'*') {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn searchable_text(record: &StoredRecord) -> Vec<&str> {
    ["message", "title", "name"]
        .into_iter()
        .filter_map(|field| record.fields.get(field).and_then(Value::as_str))
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::model::Signal;

    fn record() -> StoredRecord {
        StoredRecord {
            project_id: 1,
            id: "event".into(),
            signal: Signal::Error,
            source: "sentry".into(),
            source_id: Some("event".into()),
            issue_id: None,
            timestamp_unix_nano: 10_000_000_000,
            received_at_ms: 11_000,
            fields: BTreeMap::from([
                ("level".into(), json!("error")),
                ("message".into(), json!("Disk full on worker")),
                ("title".into(), json!("OSError: disk full")),
                ("attribute.region".into(), json!("eu")),
                ("user.id".into(), json!("42")),
                ("error.type".into(), json!("OSError")),
                ("error.value".into(), json!("disk full")),
                ("sdk.name".into(), json!("sentry.python")),
            ]),
            raw: json!({
                "level": "error",
                "message": "Disk full on worker",
                "tags": {"region": "eu"},
                "user": {"id": "42"},
                "exception": {"values": [{"type": "OSError", "value": "disk full"}]}
            }),
        }
    }

    #[test]
    fn evaluates_boolean_search() {
        let expression =
            parse("level:error region:[us,eu] AND (user.id:\"42\" OR message:*timeout)").unwrap();
        assert!(matches(&record(), &expression));
        assert!(!matches(&record(), &parse("!level:error").unwrap()));
        assert!(matches(&record(), &parse("Disk full").unwrap()));
        assert!(!matches(&record(), &parse("user.id:42").unwrap()));
        assert!(validate_fields(&parse("user.unknown:value").unwrap()).is_err());
        assert!(validate_fields(&parse("custom:value tags[region]:eu").unwrap()).is_ok());
    }

    #[test]
    fn preserves_and_before_or() {
        let expression = parse("level:warning AND region:eu OR level:error").unwrap();
        assert!(matches(&record(), &expression));
    }

    #[test]
    fn parses_numeric_and_rfc3339_time() {
        assert_eq!(parse_time_ms("10").unwrap(), 10_000);
        assert_eq!(parse_time_ms("1970-01-01T00:00:10Z").unwrap(), 10_000);
        assert_eq!(parse_duration_ms("24h").unwrap(), 86_400_000);
        assert!(parse_duration_ms("24").is_err());
    }

    #[test]
    fn aggregates_grouped_distribution() {
        let records = [10.0, 20.0, 30.0]
            .into_iter()
            .enumerate()
            .map(|(index, value)| StoredRecord {
                project_id: 1,
                id: index.to_string(),
                signal: Signal::Metric,
                source: "sentry".into(),
                source_id: None,
                issue_id: None,
                timestamp_unix_nano: (index as u64 + 1) * 1_000_000_000,
                received_at_ms: 0,
                fields: BTreeMap::from([
                    ("name".into(), json!("latency")),
                    ("type".into(), json!("distribution")),
                    ("unit".into(), json!("millisecond")),
                    ("value".into(), json!(value)),
                    ("attribute.route".into(), json!("/api")),
                ]),
                raw: json!({
                    "name": "latency",
                    "type": "distribution",
                    "unit": "millisecond",
                    "value": value,
                    "attributes": {"route": {"type": "string", "value": "/api"}}
                }),
            })
            .collect::<Vec<_>>();
        let mut ambiguous = records.clone();
        ambiguous[0].fields.insert("type".into(), json!("gauge"));
        assert!(
            aggregate_metrics(
                ambiguous,
                &MetricQueryRequest {
                    project_id: 1,
                    name: "latency".into(),
                    aggregate: "p50".into(),
                    query: String::new(),
                    group_by: Vec::new(),
                    metric_type: None,
                    unit: None,
                    start_ms: Some(0),
                    end_ms: Some(4_000),
                    interval_ms: None,
                    group_limit: None,
                },
                &Expr::All,
            )
            .unwrap_err()
            .to_string()
            .contains("ambiguous")
        );
        let response = aggregate_metrics(
            records,
            &MetricQueryRequest {
                project_id: 1,
                name: "latency".into(),
                aggregate: "p50".into(),
                query: String::new(),
                group_by: vec!["route".into()],
                metric_type: None,
                unit: None,
                start_ms: Some(0),
                end_ms: Some(4_000),
                interval_ms: Some(5_000),
                group_limit: None,
            },
            &Expr::All,
        )
        .unwrap();
        assert_eq!(response.series[0].group["route"], "/api");
        assert_eq!(response.series[0].points[0].value, 20.0);
        let samples = [
            Sample {
                timestamp_ms: 2,
                value: 2.0,
            },
            Sample {
                timestamp_ms: 1,
                value: 4.0,
            },
        ];
        assert_eq!(
            aggregate(&samples, "sum", "per_second", 2_000).unwrap(),
            3.0
        );
        assert_eq!(aggregate(&samples, "sum", "increase", 2_000).unwrap(), 6.0);
        assert_eq!(
            aggregate(&samples, "sum", "rate_per_second", 2_000).unwrap(),
            3.0
        );
        assert!(validate_aggregate("sum", "rate_per_minute").is_ok());
        assert!(validate_aggregate("counter", "sum").is_err());
        assert_eq!(aggregate(&samples, "gauge", "latest", 2_000).unwrap(), 2.0);
        assert_eq!(
            aggregate(&samples, "distribution", "p50", 2_000).unwrap(),
            3.0
        );
        let range = 300_000;
        assert!(range / automatic_interval(range) < 300);
    }

    #[test]
    fn rejects_complex_metric_aggregation_clearly() {
        let mut metric = record();
        metric.signal = Signal::Metric;
        metric.fields = BTreeMap::from([
            ("name".into(), json!("latency")),
            ("type".into(), json!("histogram")),
        ]);
        let error = aggregate_metrics(
            [metric],
            &MetricQueryRequest {
                project_id: 1,
                name: "latency".into(),
                aggregate: "sum".into(),
                query: String::new(),
                group_by: Vec::new(),
                metric_type: Some("histogram".into()),
                unit: None,
                start_ms: Some(0),
                end_ms: Some(11_000),
                interval_ms: None,
                group_limit: None,
            },
            &Expr::All,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "aggregation is not supported for histogram"
        );
    }
}
