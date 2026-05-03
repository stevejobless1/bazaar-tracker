import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import { db, getOrCreateProductId } from '../db';

// Usage: ts-node src/scripts/importLegacyBazaar.ts <path_to_csv>
const csvFilePath = process.argv[2];

if (!csvFilePath) {
  console.error("Please provide the path to the CSV file.");
  process.exit(1);
}

const resolvedPath = path.resolve(csvFilePath);

if (!fs.existsSync(resolvedPath)) {
  console.error(`File not found: ${resolvedPath}`);
  process.exit(1);
}

const FOUR_WEEKS_MS = 28 * 24 * 60 * 60 * 1000;

async function importData() {
  console.log(`Starting legacy import from ${resolvedPath}...`);
  
  let count = 0;
  let skipped = 0;
  let inserted = 0;

  // Prepared statements for different tiers
  const insertPriceStmt = db.prepare(`
    INSERT OR IGNORE INTO prices (
      timestamp, product_id, buy_price, sell_price, 
      buy_volume, sell_volume, buy_orders, sell_orders
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertHourlyStmt = db.prepare(`
    INSERT OR IGNORE INTO hourly_prices (
      timestamp, product_id, 
      buy_open, buy_high, buy_low, buy_close, 
      sell_open, sell_high, sell_low, sell_close, 
      avg_buy_volume, avg_sell_volume, 
      avg_buy_orders, avg_sell_orders,
      avg_buy_moving_week, avg_sell_moving_week
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
  `);

  const stream = fs.createReadStream(resolvedPath)
    .pipe(csv());

  const batchSize = 1000;
  let currentBatch: any[] = [];

  const processBatch = db.transaction((rows) => {
    const now = Date.now();
    for (const row of rows) {
      const internalId = getOrCreateProductId(row.product_id);
      const timestamp = new Date(row.timestamp).getTime();
      
      if (isNaN(timestamp)) continue;

      const buy = parseFloat(row.buy) || 0;
      const sell = parseFloat(row.sell) || 0;
      const buyVol = parseInt(row.buyVolume) || 0;
      const sellVol = parseInt(row.sellVolume) || 0;
      const buyOrders = parseInt(row.buyOrders) || 0;
      const sellOrders = parseInt(row.sellOrders) || 0;

      let result;
      if (now - timestamp > FOUR_WEEKS_MS) {
        // Insert into hourly table for historical data
        // Align timestamp to the start of the hour
        const hourlyTs = Math.floor(timestamp / (60 * 60 * 1000)) * (60 * 60 * 1000);
        result = insertHourlyStmt.run(
          hourlyTs,
          internalId,
          buy, buy, buy, buy, // OHLC all same
          sell, sell, sell, sell,
          buyVol, sellVol,
          buyOrders, sellOrders
        );
      } else {
        // Insert into raw prices for recent data
        result = insertPriceStmt.run(
          timestamp,
          internalId,
          buy,
          sell,
          buyVol,
          sellVol,
          buyOrders,
          sellOrders
        );
      }

      if (result.changes > 0) {
        inserted++;
      } else {
        skipped++;
      }
    }
  });

  console.log("Reading CSV and streaming to database...");

  for await (const row of stream) {
    currentBatch.push(row);
    count++;

    if (currentBatch.length >= batchSize) {
      processBatch(currentBatch);
      currentBatch = [];
      if (count % 10000 === 0) {
        process.stdout.write(`\rProcessed ${count} rows... (New: ${inserted}, Existing: ${skipped})`);
      }
    }
  }

  if (currentBatch.length > 0) {
    processBatch(currentBatch);
  }

  console.log(`\n\nImport finished!`);
  console.log(`Total rows processed: ${count}`);
  console.log(`New records inserted: ${inserted}`);
  console.log(`Existing records skipped: ${skipped}`);
}

importData().catch(err => {
  console.error("\nImport failed:", err);
  process.exit(1);
});
