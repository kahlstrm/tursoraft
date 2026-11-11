// The pages the Worker renders, and all the SQL they run. To point the
// pages at another schema, change this file only.
//
// The tables are those of the store in `crates/shop`:
// `products(product_id INTEGER PRIMARY KEY, title, price_cents, stock)`.

export type Query = { sql: string; params: SqlStorageValue[] };
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
        sql: `SELECT product_id, title, price_cents, stock FROM products
              ORDER BY product_id LIMIT 200`,
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
      `<td>${price(r.price_cents)}</td><td>${html(r.stock)}</td></tr>`,
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
        sql: `SELECT product_id, title, price_cents, stock FROM products
              WHERE product_id = CAST(? AS INTEGER)`,
        params: [product[1]],
      },
      render: ([r]) =>
        r === undefined
          ? null
          : layout(
              String(r.title),
              `<h1>${html(r.title)}</h1>
<p class="price">${price(r.price_cents)}</p>
<p class="stock">${html(r.stock)} in stock</p>
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
