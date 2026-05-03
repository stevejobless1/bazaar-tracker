import Database from 'better-sqlite3';
import path from 'path';

// Define the database path (Coolify/Docker uses environment variables for persistent volumes)
const dbPath = process.env.DB_PATH 
  ? path.resolve(process.env.DB_PATH) 
  : path.resolve(__dirname, '../bazaar.db');
export const db = new Database(dbPath);

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
    CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_unique ON prices(product_id, timestamp);

    -- New resolution tables
    CREATE TABLE IF NOT EXISTS one_min_prices (
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
    CREATE INDEX IF NOT EXISTS idx_one_min_prices_product_time ON one_min_prices(product_id, timestamp);

    CREATE TABLE IF NOT EXISTS thirty_min_prices (
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
    CREATE INDEX IF NOT EXISTS idx_thirty_min_prices_product_time ON thirty_min_prices(product_id, timestamp);

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

    CREATE TABLE IF NOT EXISTS five_min_prices (
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
    CREATE INDEX IF NOT EXISTS idx_five_min_prices_product_time ON five_min_prices(product_id, timestamp);

    CREATE TABLE IF NOT EXISTS ten_min_prices (
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
    CREATE INDEX IF NOT EXISTS idx_ten_min_prices_product_time ON ten_min_prices(product_id, timestamp);


    CREATE TABLE IF NOT EXISTS live_orders (
      product_id INTEGER PRIMARY KEY,
      buy_summary TEXT,
      sell_summary TEXT,
      FOREIGN KEY (product_id) REFERENCES products(id)
    );

    -- Table to track service heartbeats for uptime status pages
    CREATE TABLE IF NOT EXISTS service_heartbeats (
      service_name TEXT NOT NULL,
      timestamp INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_heartbeats_service_time ON service_heartbeats(service_name, timestamp);
  `);

  // Migration: Check if mayors table has the old 'timestamp' column instead of 'start_date'
  const mayorTableInfo = db.prepare("PRAGMA table_info(mayors)").all();
  const hasOldSchema = mayorTableInfo.some((col: any) => col.name === 'timestamp');
  
  if (hasOldSchema) {
    console.log("[DB] Migrating 'mayors' table to new schema...");
    db.exec("DROP TABLE mayors");
  }

  db.exec(`
    -- Table to track SkyBlock mayors
    CREATE TABLE IF NOT EXISTS mayors (
      name TEXT NOT NULL,
      start_date INTEGER NOT NULL,
      end_date INTEGER NOT NULL,
      UNIQUE(name, start_date, end_date)
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

const insertOneMinPriceStmt = db.prepare(`
  INSERT OR REPLACE INTO one_min_prices (
    timestamp, product_id, buy_open, buy_high, buy_low, buy_close, 
    sell_open, sell_high, sell_low, sell_close, avg_buy_volume, avg_sell_volume,
    avg_buy_orders, avg_sell_orders, avg_buy_moving_week, avg_sell_moving_week
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertFiveMinPriceStmt = db.prepare(`
  INSERT OR REPLACE INTO five_min_prices (
    timestamp, product_id, buy_open, buy_high, buy_low, buy_close, 
    sell_open, sell_high, sell_low, sell_close, avg_buy_volume, avg_sell_volume,
    avg_buy_orders, avg_sell_orders, avg_buy_moving_week, avg_sell_moving_week
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertTenMinPriceStmt = db.prepare(`
  INSERT OR REPLACE INTO ten_min_prices (
    timestamp, product_id, buy_open, buy_high, buy_low, buy_close, 
    sell_open, sell_high, sell_low, sell_close, avg_buy_volume, avg_sell_volume,
    avg_buy_orders, avg_sell_orders, avg_buy_moving_week, avg_sell_moving_week
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertThirtyMinPriceStmt = db.prepare(`
  INSERT OR REPLACE INTO thirty_min_prices (
    timestamp, product_id, buy_open, buy_high, buy_low, buy_close, 
    sell_open, sell_high, sell_low, sell_close, avg_buy_volume, avg_sell_volume,
    avg_buy_orders, avg_sell_orders, avg_buy_moving_week, avg_sell_moving_week
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

export function getOneMinPricesOlderThan(timestampMs: number): any[] {
  return db.prepare('SELECT * FROM one_min_prices WHERE timestamp < ?').all(timestampMs);
}

export function deleteOneMinPricesOlderThan(timestampMs: number) {
  db.prepare('DELETE FROM one_min_prices WHERE timestamp < ?').run(timestampMs);
}

export function getFiveMinPricesOlderThan(timestampMs: number): any[] {
  return db.prepare('SELECT * FROM five_min_prices WHERE timestamp < ?').all(timestampMs);
}

export function deleteFiveMinPricesOlderThan(timestampMs: number) {
  db.prepare('DELETE FROM five_min_prices WHERE timestamp < ?').run(timestampMs);
}

export function getTenMinPricesOlderThan(timestampMs: number): any[] {
  return db.prepare('SELECT * FROM ten_min_prices WHERE timestamp < ?').all(timestampMs);
}

export function deleteTenMinPricesOlderThan(timestampMs: number) {
  db.prepare('DELETE FROM ten_min_prices WHERE timestamp < ?').run(timestampMs);
}

export function getThirtyMinPricesOlderThan(timestampMs: number): any[] {
  return db.prepare('SELECT * FROM thirty_min_prices WHERE timestamp < ?').all(timestampMs);
}

export function deleteThirtyMinPricesOlderThan(timestampMs: number) {
  db.prepare('DELETE FROM thirty_min_prices WHERE timestamp < ?').run(timestampMs);
}

export const bulkInsertOneMinPrices = db.transaction((data: any[]) => {
  for (const d of data) {
    insertOneMinPriceStmt.run(
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

export const bulkInsertFiveMinPrices = db.transaction((data: any[]) => {
  for (const d of data) {
    insertFiveMinPriceStmt.run(
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

export const bulkInsertTenMinPrices = db.transaction((data: any[]) => {
  for (const d of data) {
    insertTenMinPriceStmt.run(
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

export const bulkInsertThirtyMinPrices = db.transaction((data: any[]) => {
  for (const d of data) {
    insertThirtyMinPriceStmt.run(
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

export function getOneMinHistory(productIdStr: string, limit: number = 1000) {
  const pId = getOrCreateProductId(productIdStr);
  return db.prepare('SELECT * FROM one_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(pId, limit);
}

export function getFiveMinHistory(productIdStr: string, limit: number = 1000) {
  const pId = getOrCreateProductId(productIdStr);
  return db.prepare('SELECT * FROM five_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(pId, limit);
}

export function getTenMinHistory(productIdStr: string, limit: number = 1000) {
  const pId = getOrCreateProductId(productIdStr);
  return db.prepare('SELECT * FROM ten_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(pId, limit);
}

export function getThirtyMinHistory(productIdStr: string, limit: number = 1000) {
  const pId = getOrCreateProductId(productIdStr);
  return db.prepare('SELECT * FROM thirty_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(pId, limit);
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

export function logHeartbeat(serviceName: string) {
  db.prepare('INSERT INTO service_heartbeats (service_name, timestamp) VALUES (?, ?)').run(serviceName, Date.now());
  
  // Cleanup old heartbeats (keep 90 days)
  const ninetyDaysAgo = Date.now() - (90 * 24 * 60 * 60 * 1000);
  db.prepare('DELETE FROM service_heartbeats WHERE timestamp < ?').run(ninetyDaysAgo);
}

export function getUptimeHistory(serviceName: string) {
  const oldestHeartbeat = (db.prepare('SELECT MIN(timestamp) as ts FROM service_heartbeats WHERE service_name = ?').get(serviceName) as any)?.ts;
  if (!oldestHeartbeat) return [];

  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  
  // Calculate days from the first heartbeat to today
  const firstDayDate = new Date(oldestHeartbeat).setHours(0, 0, 0, 0);
  const todayDate = new Date(now).setHours(0, 0, 0, 0);
  const daysSinceStart = Math.round((todayDate - firstDayDate) / dayMs) + 1;
  
  // Cap at a reasonable number for the UI if necessary, but the user asked for full history
  // Let's provide up to 90 days of history (matching our retention)
  const daysToFetch = Math.min(daysSinceStart, 90);
  
  const history = [];

  for (let i = 0; i < daysToFetch; i++) {
    const end = todayDate - (i * dayMs) + dayMs;
    const start = todayDate - (i * dayMs);
    
    // Count heartbeats in this 24h window
    // Assuming 1 heartbeat per minute = 1440 expected
    const count = (db.prepare('SELECT COUNT(*) as cnt FROM service_heartbeats WHERE service_name = ? AND timestamp >= ? AND timestamp < ?').get(serviceName, start, end) as any)?.cnt || 0;
    
    const uptimePct = Math.min((count / 1440) * 100, 100);
    history.push({
      date: new Date(start).toISOString().split('T')[0],
      uptimePct: +uptimePct.toFixed(2),
      status: uptimePct > 98 ? 'operational' : uptimePct > 80 ? 'degraded' : 'down'
    });
  }
  
  return history.reverse();
}

// --- MAYOR FUNCTIONS ---

export function insertMayor(name: string, startDate: number, endDate: number) {
  db.prepare('INSERT OR IGNORE INTO mayors (name, start_date, end_date) VALUES (?, ?, ?)').run(name, startDate, endDate);
}

export function getLastMayor(): { start_date: number, end_date: number, name: string } | null {
  return db.prepare('SELECT * FROM mayors ORDER BY start_date DESC LIMIT 1').get() as any || null;
}

export function getMayorsInRange(startTs: number, endTs: number): { start_date: number, end_date: number, name: string }[] {
  return db.prepare('SELECT * FROM mayors WHERE start_date >= ? AND start_date <= ? ORDER BY start_date ASC').all(startTs, endTs) as any;
}

export function vacuumDB() {
  console.log('[DB] Running VACUUM to reclaim space...');
  db.pragma('vacuum');
}


// --- STATUS / ANALYTICS FUNCTIONS ---

let cachedStats: any = null;
let lastStatsFetch = 0;
const STATS_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

export function getStatusStats(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && cachedStats && (now - lastStatsFetch < STATS_CACHE_TTL)) {
    return {
      ...cachedStats,
      cached: true,
      cacheAgeMs: now - lastStatsFetch
    };
  }

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

  const stats = {
    database: {
      sizeBytes: dbSizeBytes,
      sizeMB: +(dbSizeBytes / (1024 * 1024)).toFixed(2),
      pageCount,
      pageSize,
      tables: {
        prices: { rows: pricesCount, oldestTimestamp: oldestPrice, newestTimestamp: newestPrice },
        one_min_prices: { 
          rows: (db.prepare('SELECT COUNT(*) as cnt FROM one_min_prices').get() as any)?.cnt || 0,
          oldestTimestamp: (db.prepare('SELECT MIN(timestamp) as ts FROM one_min_prices').get() as any)?.ts || null,
          newestTimestamp: (db.prepare('SELECT MAX(timestamp) as ts FROM one_min_prices').get() as any)?.ts || null
        },
        five_min_prices: { 
          rows: (db.prepare('SELECT COUNT(*) as cnt FROM five_min_prices').get() as any)?.cnt || 0,
          oldestTimestamp: (db.prepare('SELECT MIN(timestamp) as ts FROM five_min_prices').get() as any)?.ts || null,
          newestTimestamp: (db.prepare('SELECT MAX(timestamp) as ts FROM five_min_prices').get() as any)?.ts || null
        },
        ten_min_prices: { 
          rows: (db.prepare('SELECT COUNT(*) as cnt FROM ten_min_prices').get() as any)?.cnt || 0,
          oldestTimestamp: (db.prepare('SELECT MIN(timestamp) as ts FROM ten_min_prices').get() as any)?.ts || null,
          newestTimestamp: (db.prepare('SELECT MAX(timestamp) as ts FROM ten_min_prices').get() as any)?.ts || null
        },
        thirty_min_prices: { 
          rows: (db.prepare('SELECT COUNT(*) as cnt FROM thirty_min_prices').get() as any)?.cnt || 0,
          oldestTimestamp: (db.prepare('SELECT MIN(timestamp) as ts FROM thirty_min_prices').get() as any)?.ts || null,
          newestTimestamp: (db.prepare('SELECT MAX(timestamp) as ts FROM thirty_min_prices').get() as any)?.ts || null
        },
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
      topVolumeProduct: { productId: maxVolumeProduct, volume: maxVolumeValue },
      marketVolatility: +(Math.random() * 5 + 1).toFixed(2), // Mocking for now, would need 24h diff
      totalMarketDepth: totalBuyOrders + totalSellOrders,
      topFlip: {
        productId: maxMarginProduct,
        percentage: maxMarginValue > 0 ? +((maxMarginValue / (totalMargin/lastPrices.size || 1)) * 100).toFixed(1) : 0
      }
    },
    uptime: {
      serverStartedAt: serverStartTime,
      uptimeMs: Date.now() - serverStartTime,
      history: {
        tracker: getUptimeHistory('tracker'),
        api: getUptimeHistory('api'),
        downsampler: getUptimeHistory('downsampler')
      }
    }
  };


  cachedStats = stats;
  lastStatsFetch = now;
  return { ...stats, cached: false };
}

// Track when this module was first loaded (proxy for server start)
const serverStartTime = Date.now();

export default db;
