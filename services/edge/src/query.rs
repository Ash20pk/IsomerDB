//! MongoDB query semantics over the replica's JSON documents: filters, sorts, projections and the
//! aggregation stages a read-only replica can answer from one collection. What it can't answer
//! is refused with the reason, so an application learns to send that query to the origin.
//!
//! Documents are JSON (the origin keeps projections as JSON), so the only values are null,
//! numbers, strings, booleans, objects and arrays, compared as MongoDB compares them: numbers by
//! value whatever their width, values of different types by MongoDB's type order.

use std::cmp::Ordering;

use bson::{Bson, Document};
use regex::Regex;
use serde_json::{Map, Number, Value};

#[derive(Debug)]
pub struct QueryError {
    pub code: i32,
    pub message: String,
}

pub type QResult<T> = Result<T, QueryError>;

pub fn bad(message: impl Into<String>) -> QueryError {
    QueryError { code: 2, message: message.into() } // BadValue
}

fn unsupported(what: impl std::fmt::Display) -> QueryError {
    QueryError { code: 115, message: format!("{what} is not supported by the edge replica; send this query to the origin") }
}

// ── values ───────────────────────────────────────────────────────────────────

/// A BSON value as the JSON the documents hold.
pub fn bson_to_json(b: &Bson) -> QResult<Value> {
    Ok(match b {
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Boolean(v) => Value::Bool(*v),
        Bson::Int32(v) => Value::from(*v),
        Bson::Int64(v) => Value::from(*v),
        Bson::Double(v) => Number::from_f64(*v).map(Value::Number).ok_or_else(|| bad("NaN and infinity never match"))?,
        Bson::Decimal128(d) => d
            .to_string()
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| bad("a Decimal128 that is not a finite number"))?,
        Bson::String(s) => Value::String(s.clone()),
        Bson::Array(a) => Value::Array(a.iter().map(bson_to_json).collect::<QResult<_>>()?),
        Bson::Document(d) => Value::Object(doc_to_json(d)?),
        other => return Err(bad(format!("documents here are JSON; a {:?} value never matches one", other.element_type()))),
    })
}

pub fn doc_to_json(d: &Document) -> QResult<Map<String, Value>> {
    d.iter().map(|(k, v)| Ok((k.clone(), bson_to_json(v)?))).collect()
}

/// A JSON value as BSON: integers as int32 when they fit, else int64; other numbers as doubles.
pub fn json_to_bson(v: &Value) -> Bson {
    match v {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i32::try_from(i).map(Bson::Int32).unwrap_or(Bson::Int64(i))
            } else if let Some(u) = n.as_u64() {
                Bson::Double(u as f64)
            } else {
                Bson::Double(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => Bson::String(s.clone()),
        Value::Array(a) => Bson::Array(a.iter().map(json_to_bson).collect()),
        Value::Object(o) => Bson::Document(object_to_doc(o)),
    }
}

/// A JSON document as BSON, with `_id` first as MongoDB returns it.
pub fn object_to_doc(o: &Map<String, Value>) -> Document {
    let mut d = Document::new();
    if let Some(id) = o.get("_id") {
        d.insert("_id", json_to_bson(id));
    }
    for (k, v) in o {
        if k != "_id" {
            d.insert(k.clone(), json_to_bson(v));
        }
    }
    d
}

fn type_rank(v: Option<&Value>) -> u8 {
    match v {
        None | Some(Value::Null) => 1,
        Some(Value::Number(_)) => 2,
        Some(Value::String(_)) => 3,
        Some(Value::Object(_)) => 4,
        Some(Value::Array(_)) => 5,
        Some(Value::Bool(_)) => 8,
    }
}

fn num(n: &Number) -> f64 {
    n.as_f64().unwrap_or(f64::NAN)
}

/// MongoDB's total order over values (missing sorts as null).
pub fn compare(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Some(Value::Number(x)), Some(Value::Number(y))) => match (x.as_i64(), y.as_i64()) {
            (Some(i), Some(j)) => i.cmp(&j),
            _ => num(x).partial_cmp(&num(y)).unwrap_or(Ordering::Equal),
        },
        (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),
        (Some(Value::Array(x)), Some(Value::Array(y))) => {
            for (p, q) in x.iter().zip(y) {
                let c = compare(Some(p), Some(q));
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (Some(Value::Object(x)), Some(Value::Object(y))) => {
            for ((kp, p), (kq, q)) in x.iter().zip(y) {
                let c = compare(Some(p), Some(q)).then_with(|| kp.cmp(kq));
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => Ordering::Equal,
    }
}

fn equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => compare(Some(a), Some(b)) == Ordering::Equal,
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(p, q)| equal(p, q)),
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|((kp, p), (kq, q))| kp == kq && equal(p, q))
        }
        _ => a == b,
    }
}

/// The values a dotted path reaches, stepping into every element of an array on the way.
fn values_at<'a>(v: &'a Value, path: &[&str], out: &mut Vec<&'a Value>) {
    let Some((head, rest)) = path.split_first() else {
        out.push(v);
        return;
    };
    match v {
        Value::Object(o) => {
            if let Some(next) = o.get(*head) {
                values_at(next, rest, out);
            }
        }
        Value::Array(a) => {
            if let Ok(i) = head.parse::<usize>() {
                if let Some(next) = a.get(i) {
                    values_at(next, rest, out);
                }
            }
            for e in a {
                if e.is_object() {
                    values_at(e, path, out);
                }
            }
        }
        _ => {}
    }
}

/// The first value at a path, as a sort or a group key reads it.
pub fn get_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    values_at(v, &parts, &mut out);
    out.into_iter().next()
}

// ── filters ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Nor(Vec<Filter>),
    Field(Vec<String>, Vec<Op>),
}

#[derive(Debug)]
pub enum Op {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    In(Vec<Value>),
    Nin(Vec<Value>),
    Exists(bool),
    Regex(Regex),
    Size(usize),
    All(Vec<Value>),
    ElemMatch(Box<Filter>),
    Not(Vec<Op>),
}

impl Filter {
    pub fn compile(d: &Document) -> QResult<Filter> {
        let mut all = Vec::new();
        for (k, v) in d {
            all.push(match k.as_str() {
                "$and" | "$or" | "$nor" => {
                    let Bson::Array(items) = v else { return Err(bad(format!("{k} takes an array"))) };
                    let subs = items
                        .iter()
                        .map(|i| match i {
                            Bson::Document(d) => Filter::compile(d),
                            _ => Err(bad(format!("{k} takes an array of filters"))),
                        })
                        .collect::<QResult<Vec<_>>>()?;
                    if subs.is_empty() {
                        return Err(bad(format!("{k} needs at least one filter")));
                    }
                    match k.as_str() {
                        "$and" => Filter::And(subs),
                        "$or" => Filter::Or(subs),
                        _ => Filter::Nor(subs),
                    }
                }
                "$comment" => continue,
                op if op.starts_with('$') => return Err(unsupported(format!("the query operator {op}"))),
                field => Filter::Field(field.split('.').map(str::to_owned).collect(), compile_ops(v)?),
            });
        }
        Ok(Filter::And(all))
    }

    /// For `{_id: v}` and `{_id: {$in: [...]}}`: the ids it can only match, so a lookup by key
    /// answers without a scan.
    pub fn only_ids(&self) -> Option<Vec<&Value>> {
        let Filter::And(parts) = self else { return None };
        parts.iter().find_map(|p| match p {
            Filter::Field(path, ops) if path.len() == 1 && path[0] == "_id" && ops.len() == 1 => match &ops[0] {
                Op::Eq(v) if !v.is_object() && !v.is_array() => Some(vec![v]),
                Op::In(vs) if vs.iter().all(|v| !v.is_object() && !v.is_array()) => Some(vs.iter().collect()),
                _ => None,
            },
            _ => None,
        })
    }

    pub fn matches(&self, doc: &Value) -> bool {
        match self {
            Filter::And(fs) => fs.iter().all(|f| f.matches(doc)),
            Filter::Or(fs) => fs.iter().any(|f| f.matches(doc)),
            Filter::Nor(fs) => !fs.iter().any(|f| f.matches(doc)),
            Filter::Field(path, ops) => {
                let parts: Vec<&str> = path.iter().map(String::as_str).collect();
                let mut found = Vec::new();
                values_at(doc, &parts, &mut found);
                ops.iter().all(|op| op_matches(op, &found))
            }
        }
    }
}

fn compile_ops(v: &Bson) -> QResult<Vec<Op>> {
    let Bson::Document(d) = v else {
        return Ok(vec![match v {
            Bson::RegularExpression(r) => Op::Regex(regex(&r.pattern, &r.options)?),
            _ => Op::Eq(bson_to_json(v)?),
        }]);
    };
    // An object with no operator in it is a value to compare whole.
    if !d.keys().any(|k| k.starts_with('$')) {
        return Ok(vec![Op::Eq(bson_to_json(v)?)]);
    }
    let options = d.get_str("$options").unwrap_or("");
    let mut ops = Vec::new();
    for (k, v) in d {
        let list = || -> QResult<Vec<Value>> {
            match v {
                Bson::Array(a) => a.iter().map(bson_to_json).collect(),
                _ => Err(bad(format!("{k} takes an array"))),
            }
        };
        ops.push(match k.as_str() {
            "$eq" => Op::Eq(bson_to_json(v)?),
            "$ne" => Op::Ne(bson_to_json(v)?),
            "$gt" => Op::Gt(bson_to_json(v)?),
            "$gte" => Op::Gte(bson_to_json(v)?),
            "$lt" => Op::Lt(bson_to_json(v)?),
            "$lte" => Op::Lte(bson_to_json(v)?),
            "$in" => Op::In(list()?),
            "$nin" => Op::Nin(list()?),
            "$all" => Op::All(list()?),
            "$exists" => Op::Exists(match v {
                Bson::Boolean(b) => *b,
                Bson::Int32(i) => *i != 0,
                Bson::Int64(i) => *i != 0,
                Bson::Double(f) => *f != 0.0,
                _ => true,
            }),
            "$size" => Op::Size(match v {
                Bson::Int32(i) if *i >= 0 => *i as usize,
                Bson::Int64(i) if *i >= 0 => *i as usize,
                Bson::Double(f) if *f >= 0.0 && f.fract() == 0.0 => *f as usize,
                _ => return Err(bad("$size takes a whole number")),
            }),
            "$regex" => Op::Regex(match v {
                Bson::String(p) => regex(p, options)?,
                Bson::RegularExpression(r) => regex(&r.pattern, if options.is_empty() { &r.options } else { options })?,
                _ => return Err(bad("$regex takes a string")),
            }),
            "$options" => continue,
            "$elemMatch" => match v {
                Bson::Document(d) if d.keys().all(|k| k.starts_with('$')) => {
                    // {$elemMatch: {$gt: 1}}: operators on the elements themselves.
                    Op::ElemMatch(Box::new(Filter::Field(Vec::new(), compile_ops(v)?)))
                }
                Bson::Document(d) => Op::ElemMatch(Box::new(Filter::compile(d)?)),
                _ => return Err(bad("$elemMatch takes an object")),
            },
            "$not" => match v {
                Bson::Document(_) | Bson::RegularExpression(_) => Op::Not(compile_ops(v)?),
                _ => return Err(bad("$not takes an operator expression or a regular expression")),
            },
            other => return Err(unsupported(format!("the query operator {other}"))),
        });
    }
    Ok(ops)
}

fn regex(pattern: &str, options: &str) -> QResult<Regex> {
    let mut flags = String::new();
    for c in options.chars() {
        match c {
            'i' | 'm' | 's' | 'x' => flags.push(c),
            'u' => {}
            other => return Err(bad(format!("regular expression option {other:?} is not supported"))),
        }
    }
    let full = if flags.is_empty() { pattern.to_string() } else { format!("(?{flags}){pattern}") };
    Regex::new(&full).map_err(|e| bad(format!("regular expression: {e}")))
}

/// Comparisons only match values of the same type, as in MongoDB.
fn ordered(a: &Value, b: &Value, want: fn(Ordering) -> bool) -> bool {
    type_rank(Some(a)) == type_rank(Some(b)) && want(compare(Some(a), Some(b)))
}

/// Whether any of the values (or, for an array, any element) satisfies `test`.
fn any_value(found: &[&Value], test: &dyn Fn(&Value) -> bool) -> bool {
    found.iter().any(|v| test(v) || matches!(v, Value::Array(a) if a.iter().any(test)))
}

fn op_matches(op: &Op, found: &[&Value]) -> bool {
    let eq = |want: &Value| -> bool {
        if want.is_null() && found.is_empty() {
            return true; // {f: null} matches a missing field
        }
        any_value(found, &|v| equal(v, want))
    };
    match op {
        Op::Eq(w) => eq(w),
        Op::Ne(w) => !eq(w),
        Op::In(ws) => ws.iter().any(eq),
        Op::Nin(ws) => !ws.iter().any(eq),
        Op::Gt(w) => any_value(found, &|v| ordered(v, w, Ordering::is_gt)),
        Op::Gte(w) => any_value(found, &|v| ordered(v, w, Ordering::is_ge)),
        Op::Lt(w) => any_value(found, &|v| ordered(v, w, Ordering::is_lt)),
        Op::Lte(w) => any_value(found, &|v| ordered(v, w, Ordering::is_le)),
        Op::Exists(want) => found.is_empty() != *want,
        Op::Regex(r) => any_value(found, &|v| matches!(v, Value::String(s) if r.is_match(s))),
        Op::Size(n) => found.iter().any(|v| matches!(v, Value::Array(a) if a.len() == *n)),
        Op::All(ws) => !ws.is_empty() && ws.iter().all(eq),
        Op::ElemMatch(f) => found.iter().any(|v| match v {
            Value::Array(a) => a.iter().any(|e| match &**f {
                Filter::Field(p, ops) if p.is_empty() => ops.iter().all(|op| op_matches(op, &[e])),
                f => e.is_object() && f.matches(e),
            }),
            _ => false,
        }),
        Op::Not(ops) => !ops.iter().all(|op| op_matches(op, found)),
    }
}

// ── sort, projection ─────────────────────────────────────────────────────────

pub type Sort = Vec<(String, bool)>;

pub fn compile_sort(d: &Document) -> QResult<Sort> {
    d.iter()
        .map(|(k, v)| {
            let dir = match v {
                Bson::Int32(i) => *i as f64,
                Bson::Int64(i) => *i as f64,
                Bson::Double(f) => *f,
                Bson::Document(_) => return Err(unsupported("sorting by $meta")),
                _ => return Err(bad("sort directions are 1 or -1")),
            };
            match dir {
                d if d == 1.0 => Ok((k.clone(), true)),
                d if d == -1.0 => Ok((k.clone(), false)),
                _ => Err(bad("sort directions are 1 or -1")),
            }
        })
        .collect()
}

pub fn sort(docs: &mut [Value], spec: &Sort) {
    if spec.is_empty() {
        return;
    }
    docs.sort_by(|a, b| {
        for (path, asc) in spec {
            let c = compare(get_path(a, path), get_path(b, path));
            if c != Ordering::Equal {
                return if *asc { c } else { c.reverse() };
            }
        }
        Ordering::Equal
    });
}

pub struct Projection {
    include: bool,
    paths: Vec<Vec<String>>,
    id: bool,
}

impl Projection {
    pub fn compile(d: &Document) -> QResult<Option<Projection>> {
        if d.is_empty() {
            return Ok(None);
        }
        let mut id = true;
        let mut include = None;
        let mut paths = Vec::new();
        for (k, v) in d {
            let on = match v {
                Bson::Boolean(b) => *b,
                Bson::Int32(i) => *i != 0,
                Bson::Int64(i) => *i != 0,
                Bson::Double(f) => *f != 0.0,
                _ => return Err(unsupported(format!("projecting {k} with an expression"))),
            };
            if k == "_id" {
                id = on;
                continue;
            }
            if k.starts_with('$') || k.contains(".$") {
                return Err(unsupported(format!("the projection {k}")));
            }
            if *include.get_or_insert(on) != on {
                return Err(QueryError { code: 31254, message: format!("cannot mix inclusion and exclusion in a projection ({k})") });
            }
            paths.push(k.split('.').map(str::to_owned).collect());
        }
        Ok(Some(Projection { include: include.unwrap_or(false), paths, id }))
    }

    pub fn apply(&self, doc: &Value) -> Value {
        let Value::Object(src) = doc else { return doc.clone() };
        let mut out = if self.include {
            let mut out = Map::new();
            if self.id {
                if let Some(v) = src.get("_id") {
                    out.insert("_id".into(), v.clone());
                }
            }
            for p in &self.paths {
                copy_path(src, p, &mut out);
            }
            out
        } else {
            let mut out = src.clone();
            for p in &self.paths {
                remove_path(&mut out, p);
            }
            out
        };
        if !self.id {
            out.remove("_id");
        }
        Value::Object(out)
    }
}

fn copy_path(src: &Map<String, Value>, path: &[String], out: &mut Map<String, Value>) {
    let Some((head, rest)) = path.split_first() else { return };
    let Some(v) = src.get(head) else { return };
    if rest.is_empty() {
        out.insert(head.clone(), v.clone());
        return;
    }
    match v {
        Value::Object(o) => {
            let slot = out.entry(head.clone()).or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(s) = slot {
                copy_path(o, rest, s);
            }
        }
        Value::Array(a) => {
            let items: Vec<Value> = a
                .iter()
                .filter_map(|e| match e {
                    Value::Object(o) => {
                        let mut m = Map::new();
                        copy_path(o, rest, &mut m);
                        Some(Value::Object(m))
                    }
                    _ => None,
                })
                .collect();
            out.insert(head.clone(), Value::Array(items));
        }
        _ => {}
    }
}

fn remove_path(m: &mut Map<String, Value>, path: &[String]) {
    let Some((head, rest)) = path.split_first() else { return };
    if rest.is_empty() {
        m.remove(head);
        return;
    }
    match m.get_mut(head) {
        Some(Value::Object(o)) => remove_path(o, rest),
        Some(Value::Array(a)) => {
            for e in a {
                if let Value::Object(o) = e {
                    remove_path(o, rest);
                }
            }
        }
        _ => {}
    }
}

// ── aggregation ──────────────────────────────────────────────────────────────

fn as_count(v: &Bson, what: &str) -> QResult<usize> {
    match v {
        Bson::Int32(i) if *i >= 0 => Ok(*i as usize),
        Bson::Int64(i) if *i >= 0 => Ok(*i as usize),
        Bson::Double(f) if *f >= 0.0 && f.fract() == 0.0 => Ok(*f as usize),
        _ => Err(bad(format!("{what} takes a non-negative whole number"))),
    }
}

/// An operand in $group: a field path ("$a.b") or a constant.
fn operand<'a>(e: &'a Value, doc: &'a Value) -> Option<&'a Value> {
    match e {
        Value::String(s) if s.starts_with('$') => get_path(doc, &s[1..]),
        other => Some(other),
    }
}

fn group_key(spec: &Value, doc: &Value) -> Value {
    match spec {
        Value::Object(o) => Value::Object(o.iter().map(|(k, e)| (k.clone(), operand(e, doc).cloned().unwrap_or(Value::Null))).collect()),
        e => operand(e, doc).cloned().unwrap_or(Value::Null),
    }
}

enum Acc {
    Sum { int: i64, float: f64, is_float: bool },
    Avg { sum: f64, n: u64 },
    Min(Option<Value>),
    Max(Option<Value>),
    First(Option<Value>),
    Last(Option<Value>),
    Push(Vec<Value>),
    AddToSet(Vec<Value>),
}

impl Acc {
    fn new(op: &str) -> QResult<Acc> {
        Ok(match op {
            "$sum" | "$count" => Acc::Sum { int: 0, float: 0.0, is_float: false },
            "$avg" => Acc::Avg { sum: 0.0, n: 0 },
            "$min" => Acc::Min(None),
            "$max" => Acc::Max(None),
            "$first" => Acc::First(None),
            "$last" => Acc::Last(None),
            "$push" => Acc::Push(Vec::new()),
            "$addToSet" => Acc::AddToSet(Vec::new()),
            other => return Err(unsupported(format!("the accumulator {other}"))),
        })
    }

    fn add(&mut self, v: Option<&Value>) {
        match self {
            Acc::Sum { int, float, is_float } => {
                if let Some(Value::Number(n)) = v {
                    match n.as_i64().and_then(|i| int.checked_add(i)) {
                        Some(s) if !*is_float => *int = s,
                        _ => {
                            if !*is_float {
                                *float = *int as f64;
                                *is_float = true;
                            }
                            *float += num(n);
                        }
                    }
                }
            }
            Acc::Avg { sum, n } => {
                if let Some(Value::Number(x)) = v {
                    *sum += num(x);
                    *n += 1;
                }
            }
            Acc::Min(m) => {
                if let Some(v) = v.filter(|v| !v.is_null()) {
                    if m.as_ref().map_or(true, |c| compare(Some(v), Some(c)).is_lt()) {
                        *m = Some(v.clone());
                    }
                }
            }
            Acc::Max(m) => {
                if let Some(v) = v.filter(|v| !v.is_null()) {
                    if m.as_ref().map_or(true, |c| compare(Some(v), Some(c)).is_gt()) {
                        *m = Some(v.clone());
                    }
                }
            }
            Acc::First(f) => {
                if f.is_none() {
                    *f = Some(v.cloned().unwrap_or(Value::Null));
                }
            }
            Acc::Last(l) => *l = Some(v.cloned().unwrap_or(Value::Null)),
            Acc::Push(items) => {
                if let Some(v) = v {
                    items.push(v.clone());
                }
            }
            Acc::AddToSet(items) => {
                if let Some(v) = v {
                    if !items.iter().any(|i| equal(i, v)) {
                        items.push(v.clone());
                    }
                }
            }
        }
    }

    fn value(self) -> Value {
        match self {
            Acc::Sum { int, float, is_float } => {
                if is_float {
                    Number::from_f64(float).map(Value::Number).unwrap_or(Value::Null)
                } else {
                    Value::from(int)
                }
            }
            Acc::Avg { sum, n } => {
                if n == 0 {
                    Value::Null
                } else {
                    Number::from_f64(sum / n as f64).map(Value::Number).unwrap_or(Value::Null)
                }
            }
            Acc::Min(v) | Acc::Max(v) | Acc::First(v) | Acc::Last(v) => v.unwrap_or(Value::Null),
            Acc::Push(items) | Acc::AddToSet(items) => Value::Array(items),
        }
    }
}

fn group(docs: Vec<Value>, spec: &Document) -> QResult<Vec<Value>> {
    let key_spec = bson_to_json(spec.get("_id").ok_or_else(|| bad("$group needs an _id"))?)?;
    let mut fields = Vec::new();
    for (name, acc) in spec {
        if name == "_id" {
            continue;
        }
        let Bson::Document(a) = acc else { return Err(bad(format!("$group field {name} takes an accumulator"))) };
        if a.len() != 1 {
            return Err(bad(format!("$group field {name} takes one accumulator")));
        }
        let (op, arg) = a.iter().next().unwrap();
        Acc::new(op)?;
        let arg = if op == "$count" { Value::from(1) } else { bson_to_json(arg)? };
        if let Value::Object(_) = arg {
            return Err(unsupported(format!("an expression in {op}")));
        }
        fields.push((name.clone(), op.clone(), arg));
    }
    let mut groups: Vec<(Value, Vec<Acc>)> = Vec::new();
    for doc in &docs {
        let key = group_key(&key_spec, doc);
        let i = match groups.iter().position(|(k, _)| equal(k, &key)) {
            Some(i) => i,
            None => {
                groups.push((key, fields.iter().map(|(_, op, _)| Acc::new(op).unwrap()).collect()));
                groups.len() - 1
            }
        };
        for (acc, (_, _, arg)) in groups[i].1.iter_mut().zip(&fields) {
            acc.add(operand(arg, doc));
        }
    }
    Ok(groups
        .into_iter()
        .map(|(key, accs)| {
            let mut out = Map::new();
            out.insert("_id".into(), key);
            for (acc, (name, _, _)) in accs.into_iter().zip(&fields) {
                out.insert(name.clone(), acc.value());
            }
            Value::Object(out)
        })
        .collect())
}

/// Run a pipeline over the documents the first $match (if any) selected.
pub fn pipeline(mut docs: Vec<Value>, stages: &[Document]) -> QResult<Vec<Value>> {
    for stage in stages {
        if stage.len() != 1 {
            return Err(bad("each pipeline stage is an object with one field"));
        }
        let (name, arg) = stage.iter().next().unwrap();
        let arg_doc = || match arg {
            Bson::Document(d) => Ok(d),
            _ => Err(bad(format!("{name} takes an object"))),
        };
        match name.as_str() {
            "$match" => {
                let f = Filter::compile(arg_doc()?)?;
                docs.retain(|d| f.matches(d));
            }
            "$sort" => sort(&mut docs, &compile_sort(arg_doc()?)?),
            "$skip" => {
                let n = as_count(arg, "$skip")?;
                docs.drain(..n.min(docs.len()));
            }
            "$limit" => docs.truncate(as_count(arg, "$limit")?),
            "$project" => {
                if let Some(p) = Projection::compile(arg_doc()?)? {
                    docs = docs.iter().map(|d| p.apply(d)).collect();
                }
            }
            "$count" => {
                let Bson::String(field) = arg else { return Err(bad("$count takes a field name")) };
                let n = docs.len();
                docs = if n == 0 { Vec::new() } else { vec![serde_json::json!({ field.as_str(): n })] };
            }
            "$group" => docs = group(docs, arg_doc()?)?,
            other => return Err(unsupported(format!("the aggregation stage {other}"))),
        }
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use serde_json::json;

    fn run(filter: Document, docs: &[Value]) -> Vec<i64> {
        let f = Filter::compile(&filter).unwrap();
        docs.iter().filter(|d| f.matches(d)).map(|d| d["_id"].as_i64().unwrap()).collect()
    }

    fn data() -> Vec<Value> {
        vec![
            json!({"_id": 1, "name": "ada", "age": 36, "tags": ["a", "b"], "addr": {"city": "London"}}),
            json!({"_id": 2, "name": "bob", "age": 25.5, "tags": ["b"], "addr": {"city": "Paris"}}),
            json!({"_id": 3, "name": "Cy", "tags": [], "items": [{"sku": "x", "qty": 2}, {"sku": "y", "qty": 5}]}),
            json!({"_id": 4, "name": "dee", "age": null}),
        ]
    }

    #[test]
    fn filters() {
        let d = data();
        assert_eq!(run(doc! {"age": {"$gt": 30}}, &d), vec![1]);
        assert_eq!(run(doc! {"age": {"$gte": 25, "$lt": 30}}, &d), vec![2]);
        assert_eq!(run(doc! {"age": null}, &d), vec![3, 4]);
        assert_eq!(run(doc! {"age": {"$exists": false}}, &d), vec![3]);
        assert_eq!(run(doc! {"tags": "b"}, &d), vec![1, 2]);
        assert_eq!(run(doc! {"tags": {"$size": 0}}, &d), vec![3]);
        assert_eq!(run(doc! {"tags": {"$all": ["a", "b"]}}, &d), vec![1]);
        assert_eq!(run(doc! {"addr.city": {"$in": ["Paris", "Rome"]}}, &d), vec![2]);
        assert_eq!(run(doc! {"items.sku": "y"}, &d), vec![3]);
        assert_eq!(run(doc! {"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 1}}}}, &d), vec![3]);
        assert_eq!(run(doc! {"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 2}}}}, &d), Vec::<i64>::new());
        assert_eq!(run(doc! {"name": {"$regex": "^c", "$options": "i"}}, &d), vec![3]);
        assert_eq!(run(doc! {"$or": [{"_id": 1}, {"name": "dee"}]}, &d), vec![1, 4]);
        assert_eq!(run(doc! {"name": {"$not": {"$regex": "^[ab]"}}}, &d), vec![3, 4]);
        assert_eq!(run(doc! {"age": {"$ne": 36}}, &d), vec![2, 3, 4]);
        assert_eq!(run(doc! {"name": {"$gt": 5}}, &d), Vec::<i64>::new(), "comparisons don't cross types");
        assert_eq!(run(doc! {"_id": 2.0}, &d), vec![2], "numbers compare by value");
        assert!(Filter::compile(&doc! {"$where": "1"}).is_err());
        assert!(Filter::compile(&doc! {"a": {"$near": [0, 0]}}).is_err());
    }

    #[test]
    fn ids_short_circuit() {
        let f = Filter::compile(&doc! {"_id": {"$in": [1, "x"]}}).unwrap();
        assert_eq!(f.only_ids().unwrap().len(), 2);
        assert!(Filter::compile(&doc! {"_id": {"$gt": 1}}).unwrap().only_ids().is_none());
    }

    #[test]
    fn sorts_and_projects() {
        let mut d = data();
        sort(&mut d, &compile_sort(&doc! {"age": -1, "_id": 1}).unwrap());
        assert_eq!(d.iter().map(|v| v["_id"].as_i64().unwrap()).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        let p = Projection::compile(&doc! {"name": 1, "addr.city": 1, "_id": 0}).unwrap().unwrap();
        assert_eq!(p.apply(&data()[0]), json!({"name": "ada", "addr": {"city": "London"}}));
        let p = Projection::compile(&doc! {"tags": 0, "addr": 0}).unwrap().unwrap();
        assert_eq!(p.apply(&data()[1]), json!({"_id": 2, "name": "bob", "age": 25.5}));
        assert!(Projection::compile(&doc! {"a": 1, "b": 0}).is_err());
    }

    #[test]
    fn pipelines() {
        let out = pipeline(data(), &[doc! {"$match": {"name": {"$exists": true}}}, doc! {"$group": {"_id": 1, "n": {"$sum": 1}}}]).unwrap();
        assert_eq!(out, vec![json!({"_id": 1, "n": 4})]);
        let out = pipeline(data(), &[doc! {"$group": {"_id": null, "total": {"$sum": "$age"}, "avg": {"$avg": "$age"}, "max": {"$max": "$age"}}}]).unwrap();
        assert_eq!(out, vec![json!({"_id": null, "total": 61.5, "avg": 30.75, "max": 36})]);
        let out = pipeline(data(), &[doc! {"$sort": {"_id": -1}}, doc! {"$skip": 1}, doc! {"$limit": 2}, doc! {"$project": {"name": 1}}]).unwrap();
        assert_eq!(out, vec![json!({"_id": 3, "name": "Cy"}), json!({"_id": 2, "name": "bob"})]);
        assert_eq!(pipeline(data(), &[doc! {"$count": "n"}]).unwrap(), vec![json!({"n": 4})]);
        assert!(pipeline(data(), &[doc! {"$lookup": {}}]).is_err());
    }

    #[test]
    fn numbers_keep_their_width() {
        assert_eq!(json_to_bson(&json!(7)), Bson::Int32(7));
        assert_eq!(json_to_bson(&json!(5_000_000_000i64)), Bson::Int64(5_000_000_000));
        assert_eq!(json_to_bson(&json!(1.5)), Bson::Double(1.5));
        let d = object_to_doc(json!({"b": 1, "_id": "k"}).as_object().unwrap());
        assert_eq!(d.keys().next().unwrap(), "_id");
    }
}
