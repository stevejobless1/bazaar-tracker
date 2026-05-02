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

    CREATE TABLE IF NOT EXISTS live_orders (
      product_id INTEGER PRIMARY KEY,
      buy_summary TEXT,
      sell_summary TEXT,
      FOREIGN KEY (product_id) REFERENCES products(id)
    );
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

const upsertLiveOrdersStmt = db.prepare(`
  INSERT OR REPLACE INTO live_orders (product_id, buy_summary, sell_summary)
  VALUES (?, ?, ?)
`);

const getLiveOrdersStmt = db.prepare(`
  SELECT buy_summary, sell_summary FROM live_orders WHERE product_id = ?
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

export interface LiveOrderSummaries {
  productId: string;
  buySummary: string; // JSON string
  sellSummary: string; // JSON string
}

export const bulkUpsertLiveOrders = db.transaction((orders: LiveOrderSummaries[]) => {
  for (const o of orders) {
    const pId = getOrCreateProductId(o.productId);
    upsertLiveOrdersStmt.run(pId, o.buySummary, o.sellSummary);
  }
});

export function getLiveOrders(productIdStr: string): any {
  const pId = getOrCreateProductId(productIdStr);
  const row = getLiveOrdersStmt.get(pId) as any;
  if (!row) return null;
  return {
    buy_summary: JSON.parse(row.buy_summary),
    sell_summary: JSON.parse(row.sell_summary)
  };
}

// --- STATUS / ANALYTICS FUNCTIONS ---

export function getStatusStats() {
  // Database file size info from SQLite internals
  const pageCountRow = db.prepare('PRAGMA page_count').get() as any;
  const pageSizeRow = db.prepare('PRAGMA page_size').get() as any;
  const pageCount = pageCountRow?.page_count || 0;
  const pageSize = pageSizeRow?.page_size || 0;
  const dbSizeBytes = pageCount * pageSize;

  // WAL size
  const walPageCountRow = db.prepare('PRAGMA wal_checkpoint(PASSIVE)').get() as any;

  // Row counts
  const pricesCount = (db.prepare('SELECT COUNT(*) as cnt FROM prices').get() as any)?.cnt || 0;
  const hourlyCount = (db.prepare('SELECT COUNT(*) as cnt FROM hourly_prices').get() as any)?.cnt || 0;
  const productsCount = (db.prepare('SELECT COUNT(*) as cnt FROM products').get() as any)?.cnt || 0;
  const liveOrdersCount = (db.prepare('SELECT COUNT(*) as cnt FROM live_orders').get() as any)?.cnt || 0;

  // Oldest and newest timestamps
  const oldestPrice = (db.prepare('SELECT MIN(timestamp) as ts FROM prices').get() as any)?.ts || null;
  const newestPrice = (db.prepare('SELECT MAX(timestamp) as ts FROM prices').get() as any)?.ts || null;
  const oldestHourly = (db.prepare('SELECT MIN(timestamp) as ts FROM hourly_prices').get() as any)?.ts || null;
  const newestHourly = (db.prepare('SELECT MAX(timestamp) as ts FROM hourly_prices').get() as any)?.ts || null;

  // Market analytics from latest prices
  const lastPrices = getLastRecordedPrices();
  let totalBuyVolume = 0;
  let totalSellVolume = 0;
  let totalBuyOrders = 0;
  let totalSellOrders = 0;
  let posMarginCount = 0;
  let negMarginCount = 0;
  let totalMargin = 0;
  let maxMarginProduct = '';
  let maxMarginValue = -Infinity;
  let maxVolumeProduct = '';
  let maxVolumeValue = 0;
  let totalMarketCap = 0;

  for (const [productId, p] of lastPrices.entries()) {
    const buyPrice = p.buy_price || 0;
    const sellPrice = p.sell_price || 0;
    const buyVol = p.buy_volume || 0;
    const sellVol = p.sell_volume || 0;
    const margin = buyPrice - sellPrice;

    totalBuyVolume += buyVol;
    totalSellVolume += sellVol;
    totalBuyOrders += p.buy_orders || 0;
    totalSellOrders += p.sell_orders || 0;
    totalMargin += margin;
    totalMarketCap += (buyPrice + sellPrice) / 2 * (buyVol + sellVol);

    if (margin > 0) posMarginCount++;
    else negMarginCount++;

    if (margin > maxMarginValue) {
      maxMarginValue = margin;
      maxMarginProduct = productId;
    }

    const combinedVol = buyVol + sellVol;
    if (combinedVol > maxVolumeValue) {
      maxVolumeValue = combinedVol;
      maxVolumeProduct = productId;
    }
  }

  const avgMargin = lastPrices.size > 0 ? totalMargin / lastPrices.size : 0;

  return {
    database: {
      sizeBytes: dbSizeBytes,
      sizeMB: +(dbSizeBytes / (1024 * 1024)).toFixed(2),
      pageCount,
      pageSize,
      tables: {
        prices: { rows: pricesCount, oldestTimestamp: oldestPrice, newestTimestamp: newestPrice },
        hourly_prices: { rows: hourlyCount, oldestTimestamp: oldestHourly, newestTimestamp: newestHourly },
        products: { rows: productsCount },
        live_orders: { rows: liveOrdersCount }
      }
    },
    market: {
      totalProducts: lastPrices.size,
      totalBuyVolume,
      totalSellVolume,
      totalBuyOrders,
      totalSellOrders,
      positiveMarginItems: posMarginCount,
      negativeMarginItems: negMarginCount,
      averageMargin: +avgMargin.toFixed(2),
      estimatedMarketCap: totalMarketCap,
      topMarginProduct: { productId: maxMarginProduct, margin: maxMarginValue },
      topVolumeProduct: { productId: maxVolumeProduct, volume: maxVolumeValue }
    },
    uptime: {
      serverStartedAt: serverStartTime,
      uptimeMs: Date.now() - serverStartTime
    }
  };
}

// Track when this module was first loaded (proxy for server start)
const serverStartTime = Date.now();

export default db;
