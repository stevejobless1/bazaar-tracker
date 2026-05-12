const fs = require('fs');

const appendContent = `

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
  const rows = db.prepare(\`SELECT product_id, buy_summary, sell_summary FROM live_orders WHERE product_id IN (\${placeholders})\`).all(...productIds) as any[];
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
  return db.prepare(\`SELECT * FROM \${table} WHERE product_id = ? AND timestamp < ?\`).all(productId, cutoff);
}
function deletePricesOlderThan(table: string, productId: string, cutoff: number) {
  db.prepare(\`DELETE FROM \${table} WHERE product_id = ? AND timestamp < ?\`).run(productId, cutoff);
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
  const insert = db.prepare(\`INSERT INTO \${table} (product_id, timestamp, buy_price, sell_price, buy_volume, sell_volume) VALUES (?, ?, ?, ?, ?, ?)\`);
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
`;

let content = fs.readFileSync('src/db.ts', 'utf8');
if (!content.includes('getOrCreateProductId')) {
  content = content.replace('export default db;', '') + appendContent + '\nexport default db;\n';
  fs.writeFileSync('src/db.ts', content);
}
