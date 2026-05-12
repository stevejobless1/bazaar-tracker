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

const db = new Database(dbPath);

// Enable WAL mode for performance and concurrent access (crucial for Raspberry Pi)
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

/**
 * Initialize the database schema
 */
export function initDB() {
  console.log(`[DB] Initializing database at ${dbPath}`);

  // If WIPE_DB=true, we also drop tables just in case the file unlink failed
  if (process.env.WIPE_DB === 'true') {
    console.log('[DB] ⚠️ WIPE_DB=true - Dropping all existing tables...');
    db.prepare('DROP TABLE IF EXISTS bazaar_prices').run();
    db.prepare('DROP TABLE IF EXISTS products').run();
    db.prepare('DROP TABLE IF EXISTS service_heartbeats').run();
    db.prepare('DROP TABLE IF EXISTS system_status').run();
    notifyInfo('system', 'Database Wiped', 'The database has been fully wiped and reset as requested.');
  }

  // Products table (static-ish metadata)
  db.prepare(`
    CREATE TABLE IF NOT EXISTS products (
      product_id TEXT PRIMARY KEY,
      name TEXT,
      category TEXT,
      last_updated INTEGER
    )
  `).run();

  // Bazaar prices table (the heavy lifting)
  // Indices are crucial for chart performance
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

  console.log('[DB] Schema initialized successfully.');
}

// In-memory cache of products to reduce DB hits
let productCache = new Set<string>();

export function loadAllProductsIntoCache() {
  const products = db.prepare('SELECT product_id FROM products').all() as { product_id: string }[];
  productCache = new Set(products.map(p => p.product_id));
}

/**
 * Bulk insert prices into the database
 */
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

/**
 * Get the most recent price for all products
 */
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

/**
 * Log a service heartbeat
 */
export function logHeartbeat(serviceName: string, metadata: any = {}) {
  db.prepare(`
    INSERT INTO service_heartbeats (service_name, timestamp, metadata)
    VALUES (?, ?, ?)
    ON CONFLICT(service_name) DO UPDATE SET
      timestamp = excluded.timestamp,
      metadata = excluded.metadata
  `).run(serviceName, Date.now(), JSON.stringify(metadata));
}

export function getDatabaseSize(): number {
  const fs = require('fs');
  try {
    const stats = fs.statSync(dbPath);
    return stats.size;
  } catch {
    return 0;
  }
}

export function getDatabasePath(): string {
  return dbPath;
}

export default db;
