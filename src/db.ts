import Database from 'better-sqlite3';
import path from 'path';

// Define the database path (Coolify/Docker uses environment variables for persistent volumes)
const dbPath = process.env.DB_PATH 
  ? path.resolve(process.env.DB_PATH) 
  : path.resolve(__dirname, '../bazaar.db');
const db = new Database(dbPath);

// Enable Write-Ahead Logging for better performance and concurrency
db.pragma('journal_mode = WAL');
// Synchronous mode NORMAL is safe with WAL and faster than FULL
db.pragma('synchronous = NORMAL');

// Initialize database schema
export function initDB() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id TEXT UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS prices (
      timestamp INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      buy_price REAL,
      sell_price REAL,
      buy_volume INTEGER,
      sell_volume INTEGER,
      buy_orders INTEGER,
      sell_orders INTEGER,
      buy_moving_week INTEGER,
      sell_moving_week INTEGER,
      FOREIGN KEY (product_id) REFERENCES products(id)
    );

    -- Indexes for high-speed queries on the high-resolution table
    CREATE INDEX IF NOT EXISTS idx_prices_product_time ON prices(product_id, timestamp);

    CREATE TABLE IF NOT EXISTS hourly_prices (
      timestamp INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      buy_open REAL,
      buy_high REAL,
      buy_low REAL,
      buy_close REAL,
      sell_open REAL,
      sell_high REAL,
      sell_low REAL,
      sell_close REAL,
      avg_buy_volume INTEGER,
      avg_sell_volume INTEGER,
      avg_buy_orders INTEGER,
      avg_sell_orders INTEGER,
      avg_buy_moving_week INTEGER,
      avg_sell_moving_week INTEGER,
      FOREIGN KEY (product_id) REFERENCES products(id),
      UNIQUE(product_id, timestamp)
    );

    -- Indexes for the hourly table
    CREATE INDEX IF NOT EXISTS idx_hourly_prices_product_time ON hourly_prices(product_id, timestamp);
  `);
}

// Ensure the DB is initialized
initDB();

// --- PREPARED STATEMENTS ---

const insertProductStmt = db.prepare('INSERT OR IGNORE INTO products (product_id) VALUES (?)');
const getProductIdStmt = db.prepare('SELECT id FROM products WHERE product_id = ?');
const getAllProductsStmt = db.prepare('SELECT id, product_id FROM products');
const insertPriceStmt = db.prepare(`
  INSERT INTO prices (
    timestamp, product_id, buy_price, sell_price, 
    buy_volume, sell_volume, buy_orders, sell_orders,
    buy_moving_week, sell_moving_week
  )
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);
const getLastPriceStmt = db.prepare(`
  SELECT * FROM prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT 1
`);

const insertHourlyPriceStmt = db.prepare(`
  INSERT OR REPLACE INTO hourly_prices (
    timestamp, product_id, buy_open, buy_high, buy_low, buy_close, 
    sell_open, sell_high, sell_low, sell_close, avg_buy_volume, avg_sell_volume,
    avg_buy_orders, avg_sell_orders, avg_buy_moving_week, avg_sell_moving_week
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

// --- DB HELPER FUNCTIONS ---

export interface ProductPrice {
  timestamp: number;
  productId: string;
  buyPrice: number;
  sellPrice: number;
  buyVolume: number;
  sellVolume: number;
  buyOrders: number;
  sellOrders: number;
  buyMovingWeek: number;
  sellMovingWeek: number;
}

// Keep an in-memory map of product string IDs to integer IDs to avoid DB lookups
const productCache = new Map<string, number>();

export function getOrCreateProductId(productIdStr: string): number {
  if (productCache.has(productIdStr)) {
    return productCache.get(productIdStr)!;
  }
  
  insertProductStmt.run(productIdStr);
  const row = getProductIdStmt.get(productIdStr) as { id: number };
  productCache.set(productIdStr, row.id);
  return row.id;
}

export function loadAllProductsIntoCache() {
  const rows = getAllProductsStmt.all() as { id: number; product_id: string }[];
  for (const row of rows) {
    productCache.set(row.product_id, row.id);
  }
}

// Bulk insert using a transaction for maximum speed
export const bulkInsertPrices = db.transaction((prices: ProductPrice[]) => {
  for (const p of prices) {
    const pId = getOrCreateProductId(p.productId);
    insertPriceStmt.run(
      p.timestamp,
      pId,
      p.buyPrice,
      p.sellPrice,
      p.buyVolume,
      p.sellVolume,
      p.buyOrders,
      p.sellOrders,
      p.buyMovingWeek,
      p.sellMovingWeek
    );
  }
});

export function getLastRecordedPrices(): Map<string, any> {
  const map = new Map<string, any>();
  const products = getAllProductsStmt.all() as { id: number; product_id: string }[];
  
  for (const prod of products) {
    const lastPrice = getLastPriceStmt.get(prod.id) as any;
    if (lastPrice) {
      map.set(prod.product_id, lastPrice);
    }
  }
  return map;
}

export function getRawPricesOlderThan(timestampMs: number): any[] {
  return db.prepare('SELECT * FROM prices WHERE timestamp < ?').all(timestampMs);
}

export function deleteRawPricesOlderThan(timestampMs: number) {
  db.prepare('DELETE FROM prices WHERE timestamp < ?').run(timestampMs);
}

export const bulkInsertHourlyPrices = db.transaction((hourlyData: any[]) => {
  for (const d of hourlyData) {
    insertHourlyPriceStmt.run(
      d.timestamp,
      d.product_id,
      d.buy_open, d.buy_high, d.buy_low, d.buy_close,
      d.sell_open, d.sell_high, d.sell_low, d.sell_close,
      d.avg_buy_volume, d.avg_sell_volume,
      d.avg_buy_orders, d.avg_sell_orders,
      d.avg_buy_moving_week, d.avg_sell_moving_week
    );
  }
});

export function getRecentHistory(productIdStr: string, limit: number = 1000) {
  const pId = getOrCreateProductId(productIdStr);
  return db.prepare('SELECT * FROM prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(pId, limit);
}

export function getHourlyHistory(productIdStr: string, limit: number = 1000) {
  const pId = getOrCreateProductId(productIdStr);
  return db.prepare('SELECT * FROM hourly_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(pId, limit);
}

export default db;
