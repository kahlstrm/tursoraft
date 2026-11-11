// The pages the Worker renders, and all the SQL they run. To point the
// pages at another schema, change this file only.
//
// The tables are those of the store in `crates/shop` (`SCHEMA` in
// `crates/shop/src/store.rs`). `products.stock` is the stock at setup, not
// what is left. What is left depends on how the store was set up, which
// `shop_meta` records: designs `counter` and `leader` keep a counter in
// `stock.available`, and design `units` keeps one row of `units` per unit
// for sale. The store's own pages read it the same way (`Store::catalog`,
// `Store::product`).

export type Query = { sql: string; params: SqlStorageValue[] };

/// One product with the stock still for sale, in every design.
const PRODUCTS = `SELECT p.product_id, p.title, p.price_cents,
    CASE (SELECT value FROM shop_meta WHERE key = 'reserve')
      WHEN 'units' THEN (SELECT COUNT(*) FROM units u WHERE u.product_id = p.product_id)
      ELSE s.available
    END AS available
  FROM products p JOIN stock s ON s.product_id = p.product_id`;
export type Rows = Record<string, SqlStorageValue>[];

export type Page = {
  /// The statement to run on the follower.
  query: Query;
  /// The HTML for its rows, or `null` when there is nothing to show (404).
  render: (rows: Rows) => string | null;
};

/// The page for `path`, or `null` if there is none.
export function page(path: string): Page | null {
  if (path === "/") {
    return {
      query: {
        sql: `${PRODUCTS} ORDER BY p.product_id LIMIT 200`,
        params: [],
      },
      render: (rows) =>
        layout(
          "Products",
          `<h1>Products</h1>
<table>
<tr><th>Product</th><th>Price</th><th>In stock</th></tr>
${rows
  .map(
    (r) =>
      `<tr><td><a href="/products/${html(r.product_id)}">${html(r.title)}</a></td>` +
      `<td>${price(r.price_cents)}</td><td>${html(r.available)}</td></tr>`,
  )
  .join("\n")}
</table>`,
        ),
    };
  }
  const product = path.match(/^\/products\/(-?\d{1,19})$/);
  if (product) {
    return {
      query: {
        sql: `${PRODUCTS} WHERE p.product_id = CAST(? AS INTEGER)`,
        params: [product[1]],
      },
      render: ([r]) =>
        r === undefined
          ? null
          : layout(
              String(r.title),
              `<h1>${html(r.title)}</h1>
<p class="price">${price(r.price_cents)}</p>
<p>In stock: <b id="stock">${html(r.available)}</b></p>
<p><a href="/">All products</a></p>`,
            ),
    };
  }
  return null;
}

export function layout(title: string, body: string): string {
  return `<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>${html(title)}</title></head>
<body>
${body}
</body>
</html>
`;
}

export function html(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function price(cents: SqlStorageValue): string {
  return typeof cents === "number" ? `${(cents / 100).toFixed(2)} €` : html(cents);
}
