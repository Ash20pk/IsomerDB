//! The MongoDB wire protocol at the edge: drivers and mongosh read the replicated projections as
//! collections, as they would at the origin's gateway, from memory.
//!
//! The edge is read-only and presents itself as a standalone server: find, getMore, count,
//! distinct and aggregate (the stages in `query`) are answered; writes are refused with the
//! origin to send them to. A collection is a projection, in the MongoDB database the origin
//! keeps it in.
//!
//! Authentication is the origin's, as over HTTP: the password is an IsomerDB API key, sent with
//! SASL PLAIN (`authMechanism=PLAIN&authSource=$external`; the user name is not used), checked
//! against the origin, and a collection is visible only if the key's role may read the
//! projection. SCRAM can't work here, since the edge never holds a password's verifier. PLAIN
//! sends the key as it is, so the listener takes TLS (ISOMER_EDGE_TLS_CERT, ISOMER_EDGE_TLS_KEY)
//! and refuses to start without it unless ISOMER_EDGE_MONGO_PLAINTEXT=1.
//!
//! Read your writes: a causally consistent session carries `readConcern.afterClusterTime`, the
//! cluster time of the client's last write at the origin (the origin's gateway hands it out, or
//! the session is advanced to `isomer.token_to_mongo_timestamp(token)`). The edge turns it back
//! into a write token and waits, as `X-Isomer-After` does over HTTP.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bson::spec::BinarySubtype;
use bson::{doc, Binary, Bson, Document};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::query::{self, Filter, Projection, QueryError};
use crate::{Access, Shared};

const OP_REPLY: i32 = 1;
const OP_QUERY: i32 = 2004;
const OP_MSG: i32 = 2013;
const MAX_MESSAGE: usize = 48_000_000;
const MAX_BSON: i32 = 16 * 1024 * 1024;
/// Batches stop growing past this many bytes of documents (a reply must stay under 16 MB).
const BATCH_BYTES: usize = 4 * 1024 * 1024;

static CONNECTIONS: AtomicI64 = AtomicI64::new(1);
static CURSOR_IDS: AtomicI64 = AtomicI64::new(1);

pub async fn serve(edge: Shared, listen: String, tls: Option<tokio_rustls::TlsAcceptor>) -> Result<()> {
    let socket = TcpListener::bind(&listen).await.with_context(|| format!("binding {listen}"))?;
    info!(%listen, tls = tls.is_some(), "isomer edge serving MongoDB");
    loop {
        let (stream, peer) = socket.accept().await?;
        let _ = stream.set_nodelay(true);
        let edge = edge.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let result = match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(s) => Conn::new(edge).run(s).await,
                    Err(e) => Err(e.into()),
                },
                None => Conn::new(edge).run(stream).await,
            };
            if let Err(e) = result {
                tracing::debug!(%peer, "mongodb connection closed: {e:#}");
            }
        });
    }
}

pub fn tls_acceptor(cert: &str, key: &str) -> Result<tokio_rustls::TlsAcceptor> {
    use rustls_pemfile::{certs, private_key};
    let certs = certs(&mut std::io::BufReader::new(std::fs::File::open(cert).with_context(|| cert.to_string())?))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = private_key(&mut std::io::BufReader::new(std::fs::File::open(key).with_context(|| key.to_string())?))?
        .context("no private key in ISOMER_EDGE_TLS_KEY")?;
    let config = tokio_rustls::rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key)?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

struct Cursor {
    ns: String,
    docs: std::vec::IntoIter<Value>,
}

struct Conn {
    edge: Shared,
    id: i64,
    /// The API key this connection authenticated with.
    key: Option<String>,
    cursors: HashMap<i64, Cursor>,
}

/// A command's failure, as MongoDB reports it.
struct CmdError {
    code: i32,
    name: &'static str,
    message: String,
}

impl From<QueryError> for CmdError {
    fn from(e: QueryError) -> Self {
        let name = match e.code {
            115 => "CommandNotSupported",
            31254 => "Location31254",
            _ => "BadValue",
        };
        CmdError { code: e.code, name, message: e.message }
    }
}

fn err(code: i32, name: &'static str, message: impl Into<String>) -> CmdError {
    CmdError { code, name, message: message.into() }
}

type CmdResult = std::result::Result<Document, CmdError>;

async fn read_message<S: AsyncRead + Unpin>(s: &mut S) -> Result<Option<(i32, i32, Vec<u8>)>> {
    let mut header = [0u8; 16];
    match s.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = i32::from_le_bytes(header[0..4].try_into()?) as usize;
    let request_id = i32::from_le_bytes(header[4..8].try_into()?);
    let op = i32::from_le_bytes(header[12..16].try_into()?);
    if !(16..=MAX_MESSAGE).contains(&len) {
        bail!("message length {len} out of range");
    }
    let mut body = vec![0u8; len - 16];
    s.read_exact(&mut body).await?;
    Ok(Some((request_id, op, body)))
}

fn cstring(buf: &[u8]) -> Result<(String, usize)> {
    let end = buf.iter().position(|b| *b == 0).context("unterminated string")?;
    Ok((String::from_utf8(buf[..end].to_vec())?, end + 1))
}

fn read_doc(buf: &[u8]) -> Result<(Document, usize)> {
    if buf.len() < 4 {
        bail!("short document");
    }
    let len = i32::from_le_bytes(buf[0..4].try_into()?) as usize;
    if len < 5 || len > buf.len() {
        bail!("document length {len} out of range");
    }
    Ok((Document::from_reader(&mut &buf[..len])?, len))
}

/// An OP_MSG: its body, with any document sequences folded in as arrays.
fn parse_msg(body: &[u8]) -> Result<(u32, Document)> {
    let flags = u32::from_le_bytes(body[0..4].try_into()?);
    let end = if flags & 1 != 0 { body.len() - 4 } else { body.len() }; // checksum
    let mut at = 4;
    let mut main = None;
    let mut sequences = Vec::new();
    while at < end {
        let kind = body[at];
        at += 1;
        match kind {
            0 => {
                let (d, n) = read_doc(&body[at..end])?;
                main = Some(d);
                at += n;
            }
            1 => {
                let size = i32::from_le_bytes(body[at..at + 4].try_into()?) as usize;
                let section_end = at + size;
                let (id, n) = cstring(&body[at + 4..section_end])?;
                let mut p = at + 4 + n;
                let mut docs = Vec::new();
                while p < section_end {
                    let (d, n) = read_doc(&body[p..section_end])?;
                    docs.push(Bson::Document(d));
                    p += n;
                }
                sequences.push((id, docs));
                at = section_end;
            }
            k => bail!("unknown OP_MSG section kind {k}"),
        }
    }
    let mut main = main.context("OP_MSG without a body")?;
    for (id, docs) in sequences {
        main.insert(id, docs);
    }
    Ok((flags, main))
}

fn frame(request_id: i32, response_to: i32, op: i32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + payload.len());
    out.extend_from_slice(&((16 + payload.len()) as i32).to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&op.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn wire_doc(d: &Document) -> Vec<u8> {
    let mut v = Vec::new();
    d.to_writer(&mut v).expect("a document serialises");
    v
}

impl Conn {
    fn new(edge: Shared) -> Conn {
        Conn { edge, id: CONNECTIONS.fetch_add(1, Ordering::Relaxed), key: None, cursors: HashMap::new() }
    }

    async fn run<S: AsyncRead + AsyncWrite + Unpin>(mut self, mut s: S) -> Result<()> {
        let mut next_id = 1i32;
        while let Some((request_id, op, body)) = read_message(&mut s).await? {
            let reply = match op {
                OP_MSG => {
                    let (flags, cmd) = parse_msg(&body)?;
                    let out = self.command(cmd).await;
                    if flags & 2 != 0 {
                        continue; // moreToCome: the client wants no reply
                    }
                    let mut payload = 0u32.to_le_bytes().to_vec();
                    payload.push(0);
                    payload.extend(wire_doc(&out));
                    frame(next_id, request_id, OP_MSG, &payload)
                }
                OP_QUERY => {
                    // Only the legacy handshake comes this way: a command on <db>.$cmd.
                    let (ns, n) = cstring(&body[4..])?;
                    let (mut q, _) = read_doc(&body[4 + n + 8..])?;
                    if let Ok(inner) = q.get_document("$query") {
                        q = inner.clone();
                    }
                    let out = if ns.ends_with(".$cmd") {
                        let db = ns.trim_end_matches(".$cmd").to_string();
                        q.insert("$db", db);
                        self.command(q).await
                    } else {
                        doc! {"$err": "OP_QUERY is only supported for commands", "code": 352, "ok": 0.0}
                    };
                    let mut payload = Vec::new();
                    payload.extend_from_slice(&0i32.to_le_bytes()); // responseFlags
                    payload.extend_from_slice(&0i64.to_le_bytes()); // cursorID
                    payload.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
                    payload.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
                    payload.extend(wire_doc(&out));
                    frame(next_id, request_id, OP_REPLY, &payload)
                }
                other => bail!("unsupported opcode {other}"),
            };
            next_id = next_id.wrapping_add(1);
            s.write_all(&reply).await?;
        }
        Ok(())
    }

    async fn command(&mut self, cmd: Document) -> Document {
        let Some((name, _)) = cmd.iter().next() else {
            return failure(err(59, "CommandNotFound", "empty command"));
        };
        let name = name.clone();
        match self.dispatch(&name, &cmd).await {
            Ok(mut d) => {
                d.insert("ok", 1.0);
                d
            }
            Err(e) => failure(e),
        }
    }

    async fn dispatch(&mut self, name: &str, cmd: &Document) -> CmdResult {
        let db = cmd.get_str("$db").unwrap_or("admin").to_string();
        match name.to_ascii_lowercase().as_str() {
            "hello" | "ismaster" => return Ok(self.hello(name, cmd)),
            "ping" => return Ok(doc! {}),
            "buildinfo" => return Ok(doc! {"version": "7.0.0", "versionArray": [7, 0, 0, 0], "gitVersion": "isomer-edge",
                "bits": 64, "maxBsonObjectSize": MAX_BSON, "modules": [], "storageEngines": [], "ok": 1.0}),
            "getparameter" => return Ok(doc! {"featureCompatibilityVersion": {"version": "7.0"}}),
            "getcmdlineopts" => return Ok(doc! {"argv": [], "parsed": {}}),
            "hostinfo" => return Ok(doc! {"system": {}, "os": {}, "extra": {}}),
            "whatsmyuri" => return Ok(doc! {"you": "edge"}),
            "endsessions" | "refreshsessions" | "killallsessions" => return Ok(doc! {}),
            "logout" => {
                self.key = None;
                return Ok(doc! {});
            }
            "saslstart" => return self.sasl_start(cmd).await,
            "saslcontinue" => return Err(err(18, "AuthenticationFailed", "PLAIN completes in one step")),
            "connectionstatus" => {
                let users: Vec<Bson> = match &self.key {
                    Some(_) => vec![Bson::Document(doc! {"user": "api-key", "db": "$external"})],
                    None => vec![],
                };
                return Ok(doc! {"authInfo": {"authenticatedUsers": users, "authenticatedUserRoles": []}});
            }
            _ => {}
        }
        let Some(key) = self.key.clone() else {
            return Err(err(13, "Unauthorized", format!(
                "command {name} requires authentication: connect with an IsomerDB API key as the password, \
                 authMechanism=PLAIN and authSource=$external")));
        };
        let access = self.edge.access_of(&key).await.map_err(|e| err(13, "Unauthorized", e))?;
        match name {
            "find" => self.find(&db, cmd, &key, &access).await,
            "getMore" => self.get_more(&db, cmd),
            "killCursors" => {
                let ids: Vec<Bson> = cmd.get_array("cursors").map(|a| a.to_vec()).unwrap_or_default();
                let mut killed = Vec::new();
                for id in ids {
                    if let Bson::Int64(id) = id {
                        if self.cursors.remove(&id).is_some() {
                            killed.push(Bson::Int64(id));
                        }
                    }
                }
                Ok(doc! {"cursorsKilled": killed, "cursorsNotFound": [], "cursorsAlive": [], "cursorsUnknown": []})
            }
            "count" => {
                let (docs, _) = self.select(&db, cmd, "count", "query", &key, &access).await?;
                let skip = cmd_usize(cmd, "skip")?.unwrap_or(0);
                let mut n = docs.len().saturating_sub(skip);
                if let Some(l) = cmd_usize(cmd, "limit")?.filter(|l| *l > 0) {
                    n = n.min(l);
                }
                Ok(doc! {"n": n as i64})
            }
            "distinct" => {
                let field = cmd.get_str("key").map_err(|_| err(2, "BadValue", "distinct needs a key"))?.to_string();
                let (docs, _) = self.select(&db, cmd, "distinct", "query", &key, &access).await?;
                let mut values: Vec<Value> = Vec::new();
                for d in &docs {
                    let found = query::get_path(d, &field);
                    let items: Vec<&Value> = match found {
                        Some(Value::Array(a)) => a.iter().collect(),
                        Some(v) => vec![v],
                        None => vec![],
                    };
                    for v in items {
                        if !values.iter().any(|x| query::compare(Some(x), Some(v)).is_eq()) {
                            values.push(v.clone());
                        }
                    }
                }
                values.sort_by(|a, b| query::compare(Some(a), Some(b)));
                Ok(doc! {"values": values.iter().map(query::json_to_bson).collect::<Vec<_>>()})
            }
            "aggregate" => self.aggregate(&db, cmd, &key, &access).await,
            "listCollections" => {
                let names: Vec<Bson> = self
                    .collections(&db, &access)
                    .into_iter()
                    .filter(|n| match cmd.get_document("filter").ok().and_then(|f| f.get_str("name").ok()) {
                        Some(want) => want == n,
                        None => true,
                    })
                    .map(|n| {
                        Bson::Document(doc! {"name": n, "type": "collection", "options": {},
                            "info": {"readOnly": true}, "idIndex": {"v": 2, "key": {"_id": 1}, "name": "_id_"}})
                    })
                    .collect();
                Ok(doc! {"cursor": {"id": 0i64, "ns": format!("{db}.$cmd.listCollections"), "firstBatch": names}})
            }
            "listDatabases" => {
                let mut dbs: Vec<String> = self
                    .edge
                    .replicas
                    .iter()
                    .filter(|(n, _)| access.may_read(n))
                    .filter_map(|(_, r)| r.database())
                    .collect();
                dbs.sort();
                dbs.dedup();
                let list: Vec<Bson> = dbs.into_iter().map(|d| Bson::Document(doc! {"name": d, "sizeOnDisk": 0i64, "empty": false})).collect();
                Ok(doc! {"databases": list, "totalSize": 0i64})
            }
            "listIndexes" => {
                let coll = cmd.get_str("listIndexes").unwrap_or_default();
                self.replica(&db, coll, &access)?;
                Ok(doc! {"cursor": {"id": 0i64, "ns": format!("{db}.{coll}"),
                    "firstBatch": [{"v": 2, "key": {"_id": 1}, "name": "_id_"}]}})
            }
            "dbStats" => Ok(doc! {"db": db.as_str(), "collections": self.collections(&db, &access).len() as i32}),
            "insert" | "update" | "delete" | "findAndModify" | "create" | "drop" | "createIndexes" | "dropIndexes"
            | "dropDatabase" | "renameCollection" | "bulkWrite" => Err(err(13, "Unauthorized", format!(
                "this is an edge replica of {}, which is read-only: send {name} to the origin", self.edge.origin))),
            other => Err(err(59, "CommandNotFound", format!("no such command at the edge: '{other}'"))),
        }
    }

    fn hello(&self, name: &str, cmd: &Document) -> Document {
        let mut d = doc! {
            "helloOk": true,
            "isWritablePrimary": true,
            "maxBsonObjectSize": MAX_BSON,
            "maxMessageSizeBytes": MAX_MESSAGE as i32,
            "maxWriteBatchSize": 100_000,
            "localTime": bson::DateTime::now(),
            "logicalSessionTimeoutMinutes": 30,
            "connectionId": self.id,
            "minWireVersion": 0,
            "maxWireVersion": 21,
            "readOnly": true,
        };
        if name.eq_ignore_ascii_case("ismaster") {
            d.insert("ismaster", true);
        }
        if cmd.contains_key("saslSupportedMechs") {
            d.insert("saslSupportedMechs", vec!["PLAIN"]);
        }
        d
    }

    async fn sasl_start(&mut self, cmd: &Document) -> CmdResult {
        let mechanism = cmd.get_str("mechanism").unwrap_or_default();
        if mechanism != "PLAIN" {
            return Err(err(18, "AuthenticationFailed", format!(
                "{mechanism} is not offered here: the password is an IsomerDB API key, sent with authMechanism=PLAIN \
                 and authSource=$external")));
        }
        let payload = match cmd.get("payload") {
            Some(Bson::Binary(b)) => b.bytes.clone(),
            Some(Bson::String(s)) => s.as_bytes().to_vec(),
            _ => return Err(err(18, "AuthenticationFailed", "PLAIN needs a payload")),
        };
        // authzid NUL authcid NUL password
        let parts: Vec<&[u8]> = payload.split(|b| *b == 0).collect();
        let Some(password) = parts.get(2) else {
            return Err(err(18, "AuthenticationFailed", "malformed PLAIN payload"));
        };
        let key = String::from_utf8_lossy(password).to_string();
        match self.edge.access_of(&key).await {
            Ok(_) => {
                self.key = Some(key);
                Ok(doc! {"conversationId": 1, "done": true,
                    "payload": Binary { subtype: BinarySubtype::Generic, bytes: vec![] }})
            }
            Err(e) => Err(err(18, "AuthenticationFailed", e)),
        }
    }

    fn collections(&self, db: &str, access: &Access) -> Vec<String> {
        let mut names: Vec<String> = self
            .edge
            .replicas
            .iter()
            .filter(|(n, r)| access.may_read(n) && r.database().as_deref() == Some(db))
            .map(|(n, _)| n.clone())
            .collect();
        names.sort();
        names
    }

    /// The replica behind <db>.<coll>, if this key may read it. Anything else reads as an empty
    /// collection elsewhere, but here it is an error: the edge serves only what it replicates.
    fn replica(&self, db: &str, coll: &str, access: &Access) -> std::result::Result<Arc<crate::Replica>, CmdError> {
        match self.edge.replicas.get(coll) {
            Some(r) if access.may_read(coll) && r.database().as_deref() == Some(db) => Ok(r.clone()),
            _ => Err(err(26, "NamespaceNotFound", format!(
                "{db}.{coll} is not a projection this edge replicates (or this API key can't read it)"))),
        }
    }

    /// Wait for the session's afterClusterTime, if any, then return the documents the filter
    /// selects, by key when it names ids and by scanning otherwise.
    async fn select(&self, db: &str, cmd: &Document, verb: &str, filter_field: &str, key: &str, access: &Access)
        -> std::result::Result<(Vec<Value>, String), CmdError> {
        let coll = cmd.get_str(verb).map_err(|_| err(2, "BadValue", format!("{verb} needs a collection name")))?;
        let replica = self.replica(db, coll, access)?;
        if let Ok(rc) = cmd.get_document("readConcern") {
            if let Ok(ts) = rc.get_timestamp("afterClusterTime") {
                let token = ((ts.time as i64 * 1000 + (ts.increment as i64 >> 22)) << 22) | (ts.increment as i64 & 4_194_303);
                self.edge
                    .wait_after(key, coll, &replica, &token.to_string())
                    .await
                    .map_err(|(status, msg)| match status {
                        400 => err(2, "BadValue", msg),
                        _ => err(262, "ExceededTimeLimit", msg),
                    })?;
            }
        }
        let filter = match cmd.get(filter_field) {
            None | Some(Bson::Null) => Filter::And(vec![]),
            Some(Bson::Document(f)) => Filter::compile(f)?,
            Some(_) => return Err(err(14, "TypeMismatch", format!("{filter_field} must be an object"))),
        };
        let docs = replica.docs.read().await;
        let out = match filter.only_ids() {
            Some(ids) => ids
                .into_iter()
                .filter_map(|id| docs.get(&id_key(id)))
                .filter(|d| filter.matches(d))
                .cloned()
                .collect(),
            None => docs.values().filter(|d| filter.matches(d)).cloned().collect(),
        };
        Ok((out, format!("{db}.{coll}")))
    }

    async fn find(&mut self, db: &str, cmd: &Document, key: &str, access: &Access) -> CmdResult {
        for unsupported in ["hint", "min", "max", "collation"] {
            if cmd.contains_key(unsupported) {
                return Err(err(115, "CommandNotSupported", format!("find with {unsupported} is not supported by the edge replica")));
            }
        }
        let (mut docs, ns) = self.select(db, cmd, "find", "filter", key, access).await?;
        match cmd.get_document("sort") {
            Ok(s) => query::sort(&mut docs, &query::compile_sort(s)?),
            Err(_) => query::sort(&mut docs, &vec![("_id".to_string(), true)]),
        }
        let skip = cmd_usize(cmd, "skip")?.unwrap_or(0);
        docs.drain(..skip.min(docs.len()));
        let limit = cmd_i64(cmd, "limit")?.unwrap_or(0);
        let single = cmd.get_bool("singleBatch").unwrap_or(false) || limit < 0;
        if limit != 0 {
            docs.truncate(limit.unsigned_abs() as usize);
        }
        if let Ok(p) = cmd.get_document("projection") {
            if let Some(p) = Projection::compile(p)? {
                docs = docs.iter().map(|d| p.apply(d)).collect();
            }
        }
        let batch = cmd_usize(cmd, "batchSize")?.unwrap_or(101);
        Ok(self.open_cursor(ns, docs, batch, single, "firstBatch"))
    }

    async fn aggregate(&mut self, db: &str, cmd: &Document, key: &str, access: &Access) -> CmdResult {
        if matches!(cmd.get("aggregate"), Some(Bson::Int32(1))) {
            return Err(err(115, "CommandNotSupported", "database-level aggregation is not supported by the edge replica"));
        }
        let stages: Vec<Document> = cmd
            .get_array("pipeline")
            .map_err(|_| err(2, "BadValue", "aggregate needs a pipeline"))?
            .iter()
            .map(|s| match s {
                Bson::Document(d) => Ok(d.clone()),
                _ => Err(err(2, "BadValue", "pipeline stages are objects")),
            })
            .collect::<std::result::Result<_, _>>()?;
        // A leading $match selects through the same path as find, so ids are looked up.
        let (lead, rest) = match stages.first().and_then(|s| s.get_document("$match").ok()) {
            Some(m) => (Some(m.clone()), &stages[1..]),
            None => (None, &stages[..]),
        };
        let mut select_cmd = cmd.clone();
        select_cmd.insert("filter", lead.unwrap_or_default());
        let (mut docs, ns) = self.select(db, &select_cmd, "aggregate", "filter", key, access).await?;
        query::sort(&mut docs, &vec![("_id".to_string(), true)]);
        let docs = query::pipeline(docs, rest)?;
        let batch = cmd.get_document("cursor").ok().map(|c| cmd_usize(c, "batchSize")).transpose()?.flatten().unwrap_or(101);
        Ok(self.open_cursor(ns, docs, batch, false, "firstBatch"))
    }

    fn get_more(&mut self, db: &str, cmd: &Document) -> CmdResult {
        let id = cmd.get_i64("getMore").map_err(|_| err(2, "BadValue", "getMore needs a cursor id"))?;
        let coll = cmd.get_str("collection").unwrap_or_default();
        let batch = cmd_usize(cmd, "batchSize")?.unwrap_or(usize::MAX);
        let Some(mut cursor) = self.cursors.remove(&id) else {
            return Err(err(43, "CursorNotFound", format!("cursor id {id} not found")));
        };
        if cursor.ns != format!("{db}.{coll}") {
            self.cursors.insert(id, cursor);
            return Err(err(13, "Unauthorized", "the cursor belongs to another collection"));
        }
        let (items, more) = take_batch(&mut cursor.docs, batch);
        let ns = cursor.ns.clone();
        let out_id = if more {
            self.cursors.insert(id, cursor);
            id
        } else {
            0
        };
        Ok(doc! {"cursor": {"id": out_id, "ns": ns, "nextBatch": items}})
    }

    fn open_cursor(&mut self, ns: String, docs: Vec<Value>, batch: usize, single: bool, field: &str) -> Document {
        let mut iter = docs.into_iter();
        let (items, more) = take_batch(&mut iter, batch);
        let id = if more && !single {
            let id = CURSOR_IDS.fetch_add(1, Ordering::Relaxed);
            self.cursors.insert(id, Cursor { ns: ns.clone(), docs: iter });
            id
        } else {
            0
        };
        let mut c = doc! {"id": id, "ns": ns};
        c.insert(field, items);
        doc! {"cursor": c}
    }
}

fn take_batch(iter: &mut std::vec::IntoIter<Value>, batch: usize) -> (Vec<Bson>, bool) {
    let mut items = Vec::new();
    let mut bytes = 0;
    while items.len() < batch && bytes < BATCH_BYTES {
        let Some(v) = iter.next() else { return (items, false) };
        let d = match &v {
            Value::Object(o) => query::object_to_doc(o),
            other => doc! {"value": query::json_to_bson(other)},
        };
        bytes += wire_doc(&d).len();
        items.push(Bson::Document(d));
    }
    (items, iter.len() > 0)
}

/// The replica's key for an _id: the key column as text, which is how the feed names it.
fn id_key(id: &Value) -> String {
    match id {
        Value::String(s) => s.clone(),
        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => i.to_string(),
            (None, Some(f)) if f.fract() == 0.0 && f.abs() < 9e15 => (f as i64).to_string(),
            _ => n.to_string(),
        },
        other => other.to_string(),
    }
}

fn cmd_i64(cmd: &Document, field: &str) -> std::result::Result<Option<i64>, CmdError> {
    match cmd.get(field) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Int32(i)) => Ok(Some(*i as i64)),
        Some(Bson::Int64(i)) => Ok(Some(*i)),
        Some(Bson::Double(f)) if f.fract() == 0.0 => Ok(Some(*f as i64)),
        _ => Err(err(14, "TypeMismatch", format!("{field} must be a whole number"))),
    }
}

fn cmd_usize(cmd: &Document, field: &str) -> std::result::Result<Option<usize>, CmdError> {
    match cmd_i64(cmd, field)? {
        Some(n) if n < 0 => Err(err(2, "BadValue", format!("{field} must not be negative"))),
        n => Ok(n.map(|n| n as usize)),
    }
}

fn failure(e: CmdError) -> Document {
    if e.code == 18 {
        warn!(code = e.code, "{}", e.message);
    }
    doc! {"ok": 0.0, "errmsg": e.message, "code": e.code, "codeName": e.name}
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ids_name_keys_as_the_feed_does() {
        assert_eq!(id_key(&json!(42)), "42");
        assert_eq!(id_key(&json!(42.0)), "42");
        assert_eq!(id_key(&json!("abc")), "abc");
    }

    #[test]
    fn a_cluster_time_is_the_write_token_it_came_from() {
        // isomer.token_to_mongo_timestamp: t = (token >> 22) / 1000,
        // i = (((token >> 22) % 1000) << 22) | (token & 4194303)
        let token: i64 = (1_791_900_000_123 << 22) | 77;
        let t = (token >> 22) / 1000;
        let i = (((token >> 22) % 1000) << 22) | (token & 4_194_303);
        let back = ((t * 1000 + (i >> 22)) << 22) | (i & 4_194_303);
        assert_eq!(back, token);
    }

    #[test]
    fn op_msg_sequences_fold_into_the_body() {
        let body = doc! {"insert": "c", "$db": "d"};
        let item = doc! {"a": 1};
        let mut msg = 0u32.to_le_bytes().to_vec();
        msg.push(0);
        msg.extend(wire_doc(&body));
        msg.push(1);
        let seq_docs = wire_doc(&item);
        let size = 4 + "documents".len() + 1 + seq_docs.len();
        msg.extend((size as i32).to_le_bytes());
        msg.extend(b"documents\0");
        msg.extend(seq_docs);
        let (_, d) = parse_msg(&msg).unwrap();
        assert_eq!(d.get_array("documents").unwrap().len(), 1);
    }
}
