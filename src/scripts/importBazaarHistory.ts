import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import { db, getOrCreateProductId } from '../db';

// Usage: ts-node src/scripts/importBazaarHistory.ts <path_to_csv>
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

async function importData() {
  console.log(`Starting import from ${resolvedPath}...`);
  
  let count = 0;
  let skipped = 0;
  let inserted = 0;

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO prices (
      timestamp, product_id, buy_price, sell_price, 
      buy_volume, sell_volume, buy_orders, sell_orders
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const stream = fs.createReadStream(resolvedPath)
    .pipe(csv());

  const batchSize = 10000;
  let currentBatch: any[] = [];

  const processBatch = db.transaction((rows) => {
    for (const row of rows) {
      // product_id is the internal name in the CSV (e.g., SUSPICIOUS_SCRAP)
      const internalId = getOrCreateProductId(row.product_id);
      
      // Parse ISO 8601 timestamp to Unix milliseconds
      const timestamp = new Date(row.timestamp).getTime();
      
      if (isNaN(timestamp)) {
        console.warn(`Invalid timestamp: ${row.timestamp} for product ${row.product_id}`);
        continue;
      }

      const result = insertStmt.run(
        timestamp,
        internalId,
        parseFloat(row.buy) || 0,
        parseFloat(row.sell) || 0,
        parseInt(row.buyVolume) || 0,
        parseInt(row.sellVolume) || 0,
        parseInt(row.buyOrders) || 0,
        parseInt(row.sellOrders) || 0
      );

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
      process.stdout.write(`\rProcessed ${count} rows... (New: ${inserted}, Existing: ${skipped})`);
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
