/**
 * One-time migration: moves legacy data that was incorrectly imported
 * into hourly_prices → daily_prices.
 * 
 * The legacy CSV data is daily resolution, so it belongs in daily_prices.
 * This script:
 *  1. Reads all hourly_prices records
 *  2. Groups them by (product_id, day)
 *  3. Condenses each day group into a daily OHLC candle
 *  4. Inserts into daily_prices (INSERT OR IGNORE to skip existing)
 *  5. Deletes the migrated records from hourly_prices
 *
 * Usage: node dist/scripts/migrateLegacyToDaily.js
 */

import { db, getAllProductsStmt } from '../db';

const DAY_MS = 24 * 60 * 60 * 1000;

const insertDailyStmt = db.prepare(`
  INSERT OR IGNORE INTO daily_prices (
    timestamp, product_id,
    buy_open, buy_high, buy_low, buy_close,
    sell_open, sell_high, sell_low, sell_close,
    avg_buy_volume, avg_sell_volume,
    avg_buy_orders, avg_sell_orders,
    avg_buy_moving_week, avg_sell_moving_week
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

function migrate() {
  console.log('[Migration] Starting hourly_prices → daily_prices migration...');

  const products = getAllProductsStmt.all() as { id: number; product_id: string }[];
  console.log(`[Migration] Processing ${products.length} products...`);

  let totalMigrated = 0;
  let totalDeleted = 0;

  for (const product of products) {
    const rows = db.prepare('SELECT * FROM hourly_prices WHERE product_id = ? ORDER BY timestamp ASC').all(product.id) as any[];

    if (rows.length === 0) continue;

    // Group by day
    const dayGroups = new Map<number, any[]>();
    for (const row of rows) {
      const dayTs = Math.floor(row.timestamp / DAY_MS) * DAY_MS;
      if (!dayGroups.has(dayTs)) dayGroups.set(dayTs, []);
      dayGroups.get(dayTs)!.push(row);
    }

    // Condense each day into a single daily candle and insert
    const insertBatch = db.transaction(() => {
      for (const [dayTs, dayRows] of dayGroups.entries()) {
        dayRows.sort((a, b) => a.timestamp - b.timestamp);
        const first = dayRows[0];
        const last = dayRows[dayRows.length - 1];

        let buyHigh = -Infinity, buyLow = Infinity;
        let sellHigh = -Infinity, sellLow = Infinity;
        let buyVolSum = 0, sellVolSum = 0;
        let buyOrdersSum = 0, sellOrdersSum = 0;
        let buyMWSum = 0, sellMWSum = 0;

        for (const r of dayRows) {
          const bp = r.buy_close ?? r.buy_open ?? 0;
          const sp = r.sell_close ?? r.sell_open ?? 0;
          if (bp > buyHigh) buyHigh = bp;
          if (bp < buyLow) buyLow = bp;
          if (sp > sellHigh) sellHigh = sp;
          if (sp < sellLow) sellLow = sp;
          buyVolSum += r.avg_buy_volume || 0;
          sellVolSum += r.avg_sell_volume || 0;
          buyOrdersSum += r.avg_buy_orders || 0;
          sellOrdersSum += r.avg_sell_orders || 0;
          buyMWSum += r.avg_buy_moving_week || 0;
          sellMWSum += r.avg_sell_moving_week || 0;
        }

        const n = dayRows.length;
        insertDailyStmt.run(
          dayTs,
          product.id,
          first.buy_open ?? first.buy_close,
          buyHigh,
          buyLow,
          last.buy_close ?? last.buy_open,
          first.sell_open ?? first.sell_close,
          sellHigh,
          sellLow,
          last.sell_close ?? last.sell_open,
          Math.floor(buyVolSum / n),
          Math.floor(sellVolSum / n),
          Math.floor(buyOrdersSum / n),
          Math.floor(sellOrdersSum / n),
          Math.floor(buyMWSum / n),
          Math.floor(sellMWSum / n)
        );
        totalMigrated++;
      }
    });

    insertBatch();

    // Delete the old hourly records for this product
    const deleteResult = db.prepare('DELETE FROM hourly_prices WHERE product_id = ?').run(product.id);
    totalDeleted += deleteResult.changes;
  }

  console.log(`[Migration] Complete!`);
  console.log(`  Daily candles created: ${totalMigrated}`);
  console.log(`  Hourly records removed: ${totalDeleted}`);

  // Reclaim disk space
  console.log('[Migration] Running VACUUM...');
  db.pragma('vacuum');
  console.log('[Migration] Done.');
}

migrate();
