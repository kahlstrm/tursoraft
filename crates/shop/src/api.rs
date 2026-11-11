//! The JSON bodies of the HTTP API, shared by the server, the load generator
//! and the checker.

use serde::{Deserialize, Serialize};

/// How a checkout reserves stock. Chosen once for the whole cluster at setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Reserve {
    /// One counter row per product, decremented inside the interactive
    /// checkout (ShopSim design A).
    Counter,
    /// One row per sellable unit; the checkout deletes the units it takes
    /// (design B).
    Units,
    /// The counter is decremented at the leader with one `execute` before
    /// the interactive checkout, which then claims the reservation (design C).
    Leader,
}

impl Reserve {
    pub fn parse(s: &str) -> Option<Reserve> {
        match s {
            "counter" => Some(Reserve::Counter),
            "units" => Some(Reserve::Units),
            "leader" => Some(Reserve::Leader),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Reserve::Counter => "counter",
            Reserve::Units => "units",
            Reserve::Leader => "leader",
        }
    }
}

/// What a write request did. Every variant except `Ok` and `Unknown` had no
/// effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    /// Not enough stock.
    SoldOut,
    /// The commit-time check found that another transaction changed a row
    /// this one read.
    Rejected,
    /// The instance's local transaction was aborted, or could not begin.
    Aborted,
    /// No leader, or the shopper's log position was not reached in time.
    Unavailable,
    /// The request was sent and may still take effect.
    Unknown,
    /// The request itself was wrong: empty cart, unknown product, an order
    /// that is not pending.
    Invalid,
    /// A statement failed.
    Failed,
}

impl Outcome {
    /// True when the request certainly had no effect.
    pub fn had_no_effect(self) -> bool {
        !matches!(self, Outcome::Ok | Outcome::Unknown)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupRequest {
    pub reserve: Reserve,
    /// Ordinary products, ids 1..=products.
    pub products: i64,
    pub stock: i64,
    /// Stock of the flash-sale product, id 0.
    pub flash_stock: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Product {
    pub product_id: i64,
    pub title: String,
    pub price_cents: i64,
    pub available: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddToCart {
    pub product_id: i64,
    pub quantity: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartLine {
    pub product_id: i64,
    pub title: String,
    pub quantity: i64,
    pub price_cents: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cart {
    pub cart_id: String,
    pub lines: Vec<CartLine>,
    pub total_cents: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutRequest {
    pub cart_id: String,
    /// The client chooses the order id, so a failed checkout can be checked
    /// for leaving no order behind. The server picks one when it is missing.
    #[serde(default)]
    pub order_id: Option<String>,
    /// The log position of the shopper's last write.
    #[serde(default)]
    pub after: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayRequest {
    /// Whether the simulated payment provider approves. False cancels the
    /// order and gives its stock back. When missing, the provider declines
    /// the store's configured share of payments at random.
    #[serde(default)]
    pub approved: Option<bool>,
    /// The log position of the checkout.
    #[serde(default)]
    pub after: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitRequest {
    pub voters: Vec<u64>,
    #[serde(default)]
    pub followers: Vec<u64>,
}

/// The answer to every write request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteAnswer {
    pub outcome: Outcome,
    /// The log position of the write, for reads that must see it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_cents: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    /// The node whose instance took the checkout.
    pub node_id: i64,
    pub status: String,
    pub total_cents: i64,
    pub lines: Vec<OrderLine>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderLine {
    pub line_no: i64,
    pub product_id: i64,
    pub quantity: i64,
    pub price_cents: i64,
}

/// What one instance's database holds, for the checker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Export {
    pub node_id: u64,
    pub reserve: Reserve,
    /// The applied log index before and after the reads; equal when no
    /// write came in between.
    pub index_before: u64,
    pub index_after: u64,
    pub products: Vec<StockRow>,
    pub orders: Vec<Order>,
    /// Units taken per order and product (design B), as (order_id,
    /// product_id, units).
    pub order_units: Vec<(String, i64, i64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StockRow {
    pub product_id: i64,
    pub price_cents: i64,
    /// Stock at setup.
    pub stock: i64,
    /// Units still for sale.
    pub available: i64,
    /// Units reserved at the leader and not yet claimed by an order
    /// (design C).
    pub reserved: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub node_id: u64,
    pub name: String,
    pub state: String,
    pub leader: Option<u64>,
    pub applied: Option<u64>,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub reserve: Option<Reserve>,
}
