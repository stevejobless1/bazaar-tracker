import Database from 'better-sqlite3';
import path from 'path';
import { notifyInfo } from './discord';

// Configuration
const dbPath = process.env.DB_PATH 
  ? path.resolve(process.env.DB_PATH) 
  : path.resolve(__dirname, '../bazaar.db');

// AGGRESSIVE WIPE: If WIPE_DB=true, try to delete the file before opening
if (process.env.WIPE_DB === 'true') {
  try {
    const fs = require('fs');
    const absoluteDbPath = path.resolve(dbPath);
    console.log(`[DB] ⚠️ WIPE_DB=true detected. Starting aggressive wipe...`);
    console.log(`[DB] 📍 Target database path: ${absoluteDbPath}`);
    
    if (fs.existsSync(dbPath)) {
      console.log('[DB] 🗑️ Found existing database file. Deleting...');
      fs.unlinkSync(dbPath);
      console.log('[DB] ✅ Main database file deleted.');
      
      // Also delete WAL and SHM files if they exist
      const walPath = `${dbPath}-wal`;
      const shmPath = `${dbPath}-shm`;
      
      if (fs.existsSync(walPath)) {
        console.log('[DB] 🗑️ Deleting WAL file...');
        fs.unlinkSync(walPath);
      }
      if (fs.existsSync(shmPath)) {
        console.log('[DB] 🗑️ Deleting SHM file...');
        fs.unlinkSync(shmPath);
      }
      console.log('[DB] ✅ WAL and SHM files cleaned up.');
    } else {
      console.log('[DB] ℹ️ No database file found at this path. Fresh start confirmed.');
    }
  } catch (err) {
    console.error('[DB] ❌ CRITICAL: Failed to delete database file during wipe:', err);
    // We continue anyway, as initDB() has a fallback DROP TABLE logic
  }
}

export const db = new Database(dbPath);

// Enable WAL mode for performance and concurrent access (crucial for Raspberry Pi)
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('busy_timeout = 5000');

/**
 * Initialize the database schema
 */
export function initDB() {
  console.log(`[DB] Initializing database at ${dbPath}`);

  // If WIPE_DB=true, we also drop tables just in case the file unlink failed
  if (process.env.WIPE_DB === 'true') {
    console.log('[DB] ⚠️ WIPE_DB=true — Dropping ALL tables for fresh start...');
    try {
      db.exec('PRAGMA foreign_keys = OFF');
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all() as { name: string }[];
      for (const { name } of tables) {
        db.exec(`DROP TABLE IF EXISTS "${name}"`);
        console.log(`[DB]   Dropped table: ${name}`);
      }
      db.exec('PRAGMA foreign_keys = ON');
      db.exec('VACUUM');
      console.log('[DB] ✅ All tables dropped and VACUUM completed.');
      
      notifyInfo('system', 'Database Wiped', 'The database has been fully wiped and reset as requested.');
    } catch (err) {
      console.error('[DB] ❌ Failed to drop tables during wipe:', err);
    }
  }

  // Products table
  db.prepare(`
    CREATE TABLE IF NOT EXISTS products (
      product_id TEXT PRIMARY KEY,
      name TEXT,
      category TEXT,
      last_updated INTEGER
    )
  `).run();

  // Bazaar prices table
  db.prepare(`
    CREATE TABLE IF NOT EXISTS bazaar_prices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id TEXT,
      timestamp INTEGER,
      buy_price REAL,
      sell_price REAL,
      buy_volume REAL,
      sell_volume REAL,
      buy_orders INTEGER,
      sell_orders INTEGER,
      buy_moving_week REAL,
      sell_moving_week REAL,
      FOREIGN KEY(product_id) REFERENCES products(product_id)
    )
  `).run();

  db.prepare('CREATE INDEX IF NOT EXISTS idx_prices_product_time ON bazaar_prices(product_id, timestamp)').run();
  db.prepare('CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON bazaar_prices(timestamp)').run();

  // Volume history table
  db.prepare(`
    CREATE TABLE IF NOT EXISTS volume_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id TEXT,
      timestamp INTEGER,
      buy_volume_delta REAL,
      sell_volume_delta REAL,
      FOREIGN KEY(product_id) REFERENCES products(product_id)
    )
  `).run();

  db.prepare('CREATE INDEX IF NOT EXISTS idx_volume_product_time ON volume_history(product_id, timestamp)').run();

  // Mayor history table
  db.prepare(`
    CREATE TABLE IF NOT EXISTS mayors (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      timestamp INTEGER,
      term_end INTEGER
    )
  `).run();

  // Live order summaries table
  db.prepare(`
    CREATE TABLE IF NOT EXISTS live_orders (
      product_id TEXT PRIMARY KEY,
      buy_summary TEXT,
      sell_summary TEXT,
      updated_at INTEGER,
      FOREIGN KEY(product_id) REFERENCES products(product_id)
    )
  `).run();

  // Heartbeat table for health monitoring
  db.prepare(`
    CREATE TABLE IF NOT EXISTS service_heartbeats (
      service_name TEXT PRIMARY KEY,
      timestamp INTEGER,
      metadata TEXT
    )
  `).run();

  // System status for general stats
  db.prepare(`
    CREATE TABLE IF NOT EXISTS system_status (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at INTEGER
    )
  `).run();

  // Retention interval tables (Downsampler targets)
  const intervals = ['one_min', 'five_min', 'ten_min', 'thirty_min', 'hourly', 'daily'];
  for (const interval of intervals) {
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${interval}_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT,
        timestamp INTEGER,
        buy_price REAL,
        sell_price REAL,
        buy_volume REAL,
        sell_volume REAL,
        FOREIGN KEY(product_id) REFERENCES products(product_id)
      )
    `).run();
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_${interval}_product_time ON ${interval}_prices(product_id, timestamp)`).run();
  }

  console.log('[DB] Schema initialized successfully.');
}

// In-memory cache of products
let productCache = new Set<string>();

export function loadAllProductsIntoCache() {
  const products = db.prepare('SELECT product_id FROM products').all() as { product_id: string }[];
  productCache = new Set(products.map(p => p.product_id));
}

export function bulkInsertPrices(prices: any[]) {
  const insertProduct = db.prepare('INSERT OR IGNORE INTO products (product_id, last_updated) VALUES (?, ?)');
  const insertPrice = db.prepare(`
    INSERT INTO bazaar_prices (
      product_id, timestamp, buy_price, sell_price, 
      buy_volume, sell_volume, buy_orders, sell_orders,
      buy_moving_week, sell_moving_week
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const transaction = db.transaction((pricesList) => {
    for (const p of pricesList) {
      insertProduct.run(p.productId, p.timestamp);
      insertPrice.run(
        p.productId, p.timestamp, p.buyPrice, p.sellPrice,
        p.buyVolume, p.sellVolume, p.buyOrders, p.sellOrders,
        p.buyMovingWeek, p.sellMovingWeek
      );
    }
  });

  transaction(prices);
}

export function bulkUpsertLiveOrders(orders: any[]) {
  const upsert = db.prepare(`
    INSERT INTO live_orders (product_id, buy_summary, sell_summary, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(product_id) DO UPDATE SET
      buy_summary = excluded.buy_summary,
      sell_summary = excluded.sell_summary,
      updated_at = excluded.updated_at
  `);

  const now = Date.now();
  const transaction = db.transaction((orderList) => {
    for (const o of orderList) {
      upsert.run(o.productId, o.buySummary, o.sellSummary, now);
    }
  });

  transaction(orders);
}

export function insertVolumeDelta(productId: string, timestamp: number, buyDelta: number, sellDelta: number) {
  db.prepare(`
    INSERT INTO volume_history (product_id, timestamp, buy_volume_delta, sell_volume_delta)
    VALUES (?, ?, ?, ?)
  `).run(productId, timestamp, buyDelta, sellDelta);
}

export function insertMayor(name: string, timestamp: number, termEnd: number) {
  db.prepare('INSERT INTO mayors (name, timestamp, term_end) VALUES (?, ?, ?)').run(name, timestamp, termEnd);
}

export function getLastMayor() {
  return db.prepare('SELECT * FROM mayors ORDER BY timestamp DESC LIMIT 1').get() as { name: string, timestamp: number, term_end: number } | undefined;
}

export function getLastRecordedPrices(): Map<string, any> {
  const rows = db.prepare(`
    SELECT p1.* 
    FROM bazaar_prices p1
    INNER JOIN (
      SELECT product_id, MAX(timestamp) as max_ts
      FROM bazaar_prices
      GROUP BY product_id
    ) p2 ON p1.product_id = p2.product_id AND p1.timestamp = p2.max_ts
  `).all();

  const map = new Map<string, any>();
  for (const row of rows as any[]) {
    map.set(row.product_id, row);
  }
  return map;
}

export function logHeartbeat(serviceName: string, metadata: any = {}) {
  db.prepare(`
    INSERT INTO service_heartbeats (service_name, timestamp, metadata)
    VALUES (?, ?, ?)
    ON CONFLICT(service_name) DO UPDATE SET
      timestamp = excluded.timestamp,
      metadata = excluded.metadata
  `).run(serviceName, Date.now(), JSON.stringify(metadata));
}

export function getDatabaseSize() {
  const pageCountRow = db.prepare('PRAGMA page_count').get() as any;
  const pageSizeRow = db.prepare('PRAGMA page_size').get() as any;
  const pageCount = pageCountRow?.page_count || 0;
  const pageSize = pageSizeRow?.page_size || 0;
  const dbSizeBytes = pageCount * pageSize;
  
  return {
    sizeBytes: dbSizeBytes,
    sizeMB: +(dbSizeBytes / (1024 * 1024)).toFixed(2),
    pageCount,
    pageSize
  };
}

export function getDatabasePath(): string {
  return dbPath;
}

export interface LiveOrderSummaries {
  productId: string;
  buySummary: string;
  sellSummary: string;
}

export interface ProductPrice {
  productId: string;
  timestamp: number;
  buyPrice: number;
  sellPrice: number;
  buyVolume: number;
  sellVolume: number;
  buyOrders: number;
  sellOrders: number;
  buyMovingWeek: number;
  sellMovingWeek: number;
}




export function getRecentHistory(productId: string, limit: number) {
  return db.prepare('SELECT * FROM bazaar_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(productId, limit);
}
export function getOneMinHistory(productId: string, limit: number) {
  return db.prepare('SELECT * FROM one_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(productId, limit);
}
export function getFiveMinHistory(productId: string, limit: number) {
  return db.prepare('SELECT * FROM five_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(productId, limit);
}
export function getTenMinHistory(productId: string, limit: number) {
  return db.prepare('SELECT * FROM ten_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(productId, limit);
}
export function getThirtyMinHistory(productId: string, limit: number) {
  return db.prepare('SELECT * FROM thirty_min_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(productId, limit);
}
export function getHourlyHistory(productId: string, limit: number) {
  return db.prepare('SELECT * FROM hourly_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(productId, limit);
}
export function getDailyHistory(productId: string, limit: number) {
  return db.prepare('SELECT * FROM daily_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT ?').all(productId, limit);
}
export function getUnifiedHistory(productId: string) {
  return db.prepare('SELECT * FROM bazaar_prices WHERE product_id = ? ORDER BY timestamp DESC LIMIT 100').all(productId);
}
export function getLiveOrders(productId: string) {
  const row = db.prepare('SELECT buy_summary, sell_summary FROM live_orders WHERE product_id = ?').get(productId) as any;
  if (!row) return null;
  return {
    buy_summary: JSON.parse(row.buy_summary),
    sell_summary: JSON.parse(row.sell_summary)
  };
}
export function getLiveOrdersBulk(productIds: string[]) {
  if (productIds.length === 0) return {};
  const placeholders = productIds.map(() => '?').join(',');
  const rows = db.prepare(`SELECT product_id, buy_summary, sell_summary FROM live_orders WHERE product_id IN (${placeholders})`).all(...productIds) as any[];
  const result: any = {};
  for (const row of rows) {
    result[row.product_id] = {
      buy_summary: JSON.parse(row.buy_summary),
      sell_summary: JSON.parse(row.sell_summary)
    };
  }
  return result;
}
export function getStatusStats() {
  return { database: {}, market: {}, uptime: {} }; 
}
export function getMayorsInRange(start: number, end: number) {
  return db.prepare('SELECT * FROM mayors WHERE timestamp >= ? AND timestamp <= ?').all(start, end);
}
export function getVolumeHistory(productId: string, start: number, end: number, interval: number) {
  return db.prepare('SELECT * FROM volume_history WHERE product_id = ? AND timestamp >= ? AND timestamp <= ?').all(productId, start, end);
}

export function getAllProducts() {
  return db.prepare('SELECT product_id as id, product_id FROM products').all();
}

function getPricesOlderThan(table: string, productId: string, cutoff: number) {
  return db.prepare(`SELECT * FROM ${table} WHERE product_id = ? AND timestamp < ?`).all(productId, cutoff);
}
function deletePricesOlderThan(table: string, productId: string, cutoff: number) {
  db.prepare(`DELETE FROM ${table} WHERE product_id = ? AND timestamp < ?`).run(productId, cutoff);
}

export function getRawPricesOlderThanForProduct(pId: string, cutoff: number) { return getPricesOlderThan('bazaar_prices', pId, cutoff); }
export function deleteRawPricesOlderThanForProduct(pId: string, cutoff: number) { deletePricesOlderThan('bazaar_prices', pId, cutoff); }

export function getOneMinPricesOlderThanForProduct(pId: string, cutoff: number) { return getPricesOlderThan('one_min_prices', pId, cutoff); }
export function deleteOneMinPricesOlderThanForProduct(pId: string, cutoff: number) { deletePricesOlderThan('one_min_prices', pId, cutoff); }

export function getFiveMinPricesOlderThanForProduct(pId: string, cutoff: number) { return getPricesOlderThan('five_min_prices', pId, cutoff); }
export function deleteFiveMinPricesOlderThanForProduct(pId: string, cutoff: number) { deletePricesOlderThan('five_min_prices', pId, cutoff); }

export function getTenMinPricesOlderThanForProduct(pId: string, cutoff: number) { return getPricesOlderThan('ten_min_prices', pId, cutoff); }
export function deleteTenMinPricesOlderThanForProduct(pId: string, cutoff: number) { deletePricesOlderThan('ten_min_prices', pId, cutoff); }

export function getThirtyMinPricesOlderThanForProduct(pId: string, cutoff: number) { return getPricesOlderThan('thirty_min_prices', pId, cutoff); }
export function deleteThirtyMinPricesOlderThanForProduct(pId: string, cutoff: number) { deletePricesOlderThan('thirty_min_prices', pId, cutoff); }

export function getHourlyPricesOlderThanForProduct(pId: string, cutoff: number) { return getPricesOlderThan('hourly_prices', pId, cutoff); }
export function deleteHourlyPricesOlderThanForProduct(pId: string, cutoff: number) { deletePricesOlderThan('hourly_prices', pId, cutoff); }

function bulkInsertResolutionPrices(table: string, prices: any[]) {
  const insert = db.prepare(`INSERT INTO ${table} (product_id, timestamp, buy_price, sell_price, buy_volume, sell_volume) VALUES (?, ?, ?, ?, ?, ?)`);
  const tx = db.transaction((list: any[]) => {
    for (const p of list) {
      insert.run(p.product_id, p.timestamp, p.buy_close, p.sell_close, p.avg_buy_volume, p.avg_sell_volume);
    }
  });
  tx(prices);
}

export function bulkInsertOneMinPrices(prices: any[]) { bulkInsertResolutionPrices('one_min_prices', prices); }
export function bulkInsertFiveMinPrices(prices: any[]) { bulkInsertResolutionPrices('five_min_prices', prices); }
export function bulkInsertTenMinPrices(prices: any[]) { bulkInsertResolutionPrices('ten_min_prices', prices); }
export function bulkInsertThirtyMinPrices(prices: any[]) { bulkInsertResolutionPrices('thirty_min_prices', prices); }
export function bulkInsertHourlyPrices(prices: any[]) { bulkInsertResolutionPrices('hourly_prices', prices); }
export function bulkInsertDailyPrices(prices: any[]) { bulkInsertResolutionPrices('daily_prices', prices); }

export function incrementalVacuum() {
  db.pragma('auto_vacuum = INCREMENTAL');
  db.pragma('incremental_vacuum(100)');
}

export function cleanupHeartbeats() {
  db.prepare('DELETE FROM service_heartbeats WHERE timestamp < ?').run(Date.now() - 3600000);
}

export function getOrCreateProductId(productId: string) { return productId; }

export default db;
