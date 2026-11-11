//! The HTTP API of one store instance.
//!
//! The public port serves shoppers: pages and the JSON API under `/api`.
//! The admin port forms the cluster, sets up the store and exports what the
//! checker needs; bind it to an address only operators reach.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::Stream;
use serde::Deserialize;
use tursoraft_node::{Error, Event};
use tursoraft_raft::ClusterConfig;

use crate::api::{
    AddToCart, CheckoutRequest, InitRequest, Outcome, PayRequest, SetupRequest, Status, WriteAnswer,
};
use crate::store::{valid_id, Store, UNIT_ID_BASE};

pub struct App {
    pub store: Store,
    pub cluster: ClusterConfig,
}

type Shared = Arc<App>;

pub fn public(app: Shared) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/products/{id}", get(product_page))
        .route("/api/status", get(status))
        .route("/api/products", get(products))
        .route("/api/products/{id}", get(product))
        .route("/api/products/{id}/stock", get(stock_events))
        .route("/api/carts/{cart_id}", get(cart))
        .route("/api/carts/{cart_id}/lines", post(add_to_cart))
        .route("/api/checkout", post(checkout))
        .route("/api/orders/{order_id}", get(order))
        .route("/api/orders/{order_id}/pay", post(pay))
        .with_state(app)
}

pub fn admin(app: Shared) -> Router {
    Router::new()
        .route("/admin/status", get(status))
        .route("/admin/init", post(init))
        .route("/admin/setup", post(setup))
        .route("/admin/export", get(export))
        .with_state(app)
}

#[derive(Deserialize)]
struct After {
    #[serde(default)]
    after: u64,
}

#[derive(Deserialize)]
struct Page {
    #[serde(default)]
    offset: i64,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_limit() -> i64 {
    24
}

async fn status(State(app): State<Shared>) -> Json<Status> {
    let s = app.store.node.raft().status();
    Json(Status {
        node_id: s.id,
        name: app.store.name.clone(),
        state: s.state,
        leader: s.leader,
        applied: s.applied,
        voters: s.voters,
        learners: s.learners,
        reserve: app.store.reserve().await,
    })
}

async fn products(State(app): State<Shared>, Query(p): Query<Page>) -> Response {
    read_answer(app.store.catalog(p.offset, p.limit).await)
}

async fn product(State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    match app.store.product(id).await {
        Ok(Some(p)) => Json(p).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "no such product").into_response(),
        Err(e) => read_error(&e),
    }
}

/// Server-sent events: the stock of one product now, then again after every
/// applied transaction that changed it. Fed by [`tursoraft_node::Node::subscribe`].
async fn stock_events(
    State(app): State<Shared>,
    Path(id): Path<i64>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    // Subscribe before the first read, so no change falls between the two.
    let (after, events) = app.store.node.subscribe();
    let stream = futures_util::stream::unfold(
        (app, events, Some(after)),
        move |(app, mut events, mut first)| async move {
            loop {
                let index = match first.take() {
                    Some(index) => index,
                    None => match events.recv().await? {
                        Event::Changes(c)
                            if c.rows.iter().any(|r| touches(&r.table, r.rowid, id)) =>
                        {
                            c.index
                        }
                        Event::Changes(_) => continue,
                        Event::Reset { index } => index,
                        Event::Lagged { .. } => app.store.node.stats().applied_index,
                    },
                };
                let data = match app.store.available(id).await {
                    Ok(available) => serde_json::json!({
                        "product_id": id, "available": available, "index": index,
                    }),
                    Err(e) => serde_json::json!({ "product_id": id, "error": e.to_string() }),
                };
                let event = SseEvent::default().data(data.to_string());
                return Some((Ok(event), (app, events, None)));
            }
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

/// Whether a changed row of `table` belongs to the stock of `product_id`.
fn touches(table: &str, rowid: i64, product_id: i64) -> bool {
    match table {
        "stock" => rowid == product_id,
        "units" => rowid / UNIT_ID_BASE == product_id,
        _ => false,
    }
}

async fn cart(
    State(app): State<Shared>,
    Path(cart_id): Path<String>,
    Query(a): Query<After>,
) -> Response {
    if !valid_id(&cart_id) {
        return bad_id();
    }
    read_answer(app.store.cart(&cart_id, a.after).await)
}

async fn add_to_cart(
    State(app): State<Shared>,
    Path(cart_id): Path<String>,
    Json(req): Json<AddToCart>,
) -> Response {
    if !valid_id(&cart_id) {
        return bad_id();
    }
    write_answer(
        app.store
            .add_to_cart(&cart_id, req.product_id, req.quantity)
            .await,
    )
}

async fn checkout(State(app): State<Shared>, Json(req): Json<CheckoutRequest>) -> Response {
    if !valid_id(&req.cart_id) {
        return bad_id();
    }
    write_answer(app.store.checkout(&req).await)
}

async fn order(
    State(app): State<Shared>,
    Path(order_id): Path<String>,
    Query(a): Query<After>,
) -> Response {
    if !valid_id(&order_id) {
        return bad_id();
    }
    match app.store.order(&order_id, a.after).await {
        Ok(Some(o)) => Json(o).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "no such order").into_response(),
        Err(e) => read_error(&e),
    }
}

async fn pay(
    State(app): State<Shared>,
    Path(order_id): Path<String>,
    Json(req): Json<PayRequest>,
) -> Response {
    if !valid_id(&order_id) {
        return bad_id();
    }
    write_answer(app.store.pay(&order_id, req.approved, req.after).await)
}

/// Forms the cluster: `voters` vote, `followers` follow without voting.
/// Call on one voter, once.
async fn init(State(app): State<Shared>, Json(req): Json<InitRequest>) -> Response {
    let addr = |id: &u64| app.cluster.nodes.get(id).cloned().ok_or(*id);
    let voters: Result<BTreeMap<u64, String>, u64> = req
        .voters
        .iter()
        .map(|id| addr(id).map(|a| (*id, a)))
        .collect();
    let voters = match voters {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => return (StatusCode::BAD_REQUEST, "no voters".to_string()).into_response(),
        Err(id) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("node {id} is not in the config"),
            )
                .into_response()
        }
    };
    let raft = app.store.node.raft();
    if let Err(e) = raft.initialize(voters).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    for id in &req.followers {
        let Ok(a) = addr(id) else {
            return (
                StatusCode::BAD_REQUEST,
                format!("node {id} is not in the config"),
            )
                .into_response();
        };
        if let Err(e) = raft.add_follower(*id, a).await {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }
    Json(raft.status()).into_response()
}

async fn setup(State(app): State<Shared>, Json(req): Json<SetupRequest>) -> Response {
    match app.store.setup(&req).await {
        Ok(index) => Json(serde_json::json!({ "index": index })).into_response(),
        Err(e) => (StatusCode::CONFLICT, e).into_response(),
    }
}

async fn export(State(app): State<Shared>, Query(a): Query<After>) -> Response {
    read_answer(app.store.export(a.after).await)
}

async fn home(State(app): State<Shared>) -> Response {
    let products = match app.store.catalog(0, 24).await {
        Ok(p) => p,
        Err(e) => return read_error(&e),
    };
    let s = app.store.node.raft().status();
    let mut rows = String::new();
    for p in &products {
        rows += &format!(
            "<tr><td><a href=\"/products/{id}\">{title}</a></td><td>{price}</td><td>{available}</td></tr>\n",
            id = p.product_id,
            title = escape(&p.title),
            price = cents(p.price_cents),
            available = p.available,
        );
    }
    Html(format!(
        "<!doctype html><meta charset=utf-8><title>tursoraft shop</title>\
         <style>body{{font-family:sans-serif;max-width:40em;margin:2em auto}}td{{padding:0 1em}}</style>\
         <h1>tursoraft shop</h1><p>Instance {name} (node {id}, {state}, leader {leader}, applied log index {applied}). \
         Read locally.</p><table><tr><th>Product</th><th>Price</th><th>In stock</th></tr>\n{rows}</table>",
        name = escape(&app.store.name),
        id = s.id,
        state = s.state,
        leader = s.leader.map_or("unknown".to_string(), |l| l.to_string()),
        applied = s.applied.unwrap_or(0),
    ))
    .into_response()
}

/// A product page whose stock updates live, and buttons that buy one.
async fn product_page(State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let p = match app.store.product(id).await {
        Ok(Some(p)) => p,
        Ok(None) => return (StatusCode::NOT_FOUND, "no such product").into_response(),
        Err(e) => return read_error(&e),
    };
    Html(format!(
        r#"<!doctype html><meta charset=utf-8><title>{title}</title>
<style>body{{font-family:sans-serif;max-width:40em;margin:2em auto}}pre{{background:#eee;padding:1em}}</style>
<p><a href="/">All products</a></p>
<h1>{title}</h1>
<p>{price}. In stock: <b id=stock>{available}</b> <small id=index></small></p>
<p><button onclick="add()">Add to cart</button> <button onclick="buy()">Check out and pay</button></p>
<pre id=log></pre>
<script>
const id = {id};
let cart = localStorage.cart || (localStorage.cart = 'c-' + Math.random().toString(36).slice(2));
let after = Number(localStorage.after || 0);
const log = (x) => document.getElementById('log').textContent = JSON.stringify(x, null, 2);
new EventSource('/api/products/' + id + '/stock').onmessage = (e) => {{
  const d = JSON.parse(e.data);
  document.getElementById('stock').textContent = d.available;
  document.getElementById('index').textContent = '(log index ' + d.index + ')';
}};
async function post(url, body) {{
  const a = await (await fetch(url, {{method: 'POST', headers: {{'content-type': 'application/json'}}, body: JSON.stringify(body)}})).json();
  if (a.index) localStorage.after = after = Math.max(after, a.index);
  return a;
}}
async function add() {{
  await post('/api/carts/' + cart + '/lines', {{product_id: id, quantity: 1}});
  log(await (await fetch('/api/carts/' + cart + '?after=' + after)).json());
}}
async function buy() {{
  const c = await post('/api/checkout', {{cart_id: cart, after}});
  log(c);
  if (c.outcome === 'ok') log(await post('/api/orders/' + c.order_id + '/pay', {{after}}));
}}
</script>"#,
        title = escape(&p.title),
        price = cents(p.price_cents),
        available = p.available,
    ))
    .into_response()
}

fn read_answer<T: serde::Serialize>(r: Result<T, Error>) -> Response {
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => read_error(&e),
    }
}

fn read_error(e: &Error) -> Response {
    let code = match e {
        Error::ReadTimeout(_) | Error::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, e.to_string()).into_response()
}

fn write_answer(a: WriteAnswer) -> Response {
    let code = match a.outcome {
        Outcome::Ok => StatusCode::OK,
        Outcome::SoldOut | Outcome::Rejected => StatusCode::CONFLICT,
        Outcome::Aborted | Outcome::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Outcome::Unknown => StatusCode::GATEWAY_TIMEOUT,
        Outcome::Invalid => StatusCode::BAD_REQUEST,
        Outcome::Failed => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(a)).into_response()
}

fn bad_id() -> Response {
    (
        StatusCode::BAD_REQUEST,
        "an id is 1 to 64 letters, digits, '-' or '_'",
    )
        .into_response()
}

fn cents(c: i64) -> String {
    format!("€{}.{:02}", c / 100, c % 100)
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
