import { 
  getRawPricesOlderThanForProduct,
  deleteRawPricesOlderThanForProduct,
  getOneMinPricesOlderThanForProduct,
  deleteOneMinPricesOlderThanForProduct,
  getFiveMinPricesOlderThanForProduct,
  deleteFiveMinPricesOlderThanForProduct,
  getTenMinPricesOlderThanForProduct,
  deleteTenMinPricesOlderThanForProduct,
  getThirtyMinPricesOlderThanForProduct,
  deleteThirtyMinPricesOlderThanForProduct,
  bulkInsertOneMinPrices,
  bulkInsertFiveMinPrices,
  bulkInsertTenMinPrices,
  bulkInsertThirtyMinPrices,
  bulkInsertHourlyPrices,
  vacuumDB,
  logHeartbeat,
  getAllProductsStmt
} from './db';

const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const TWO_WEEKS_MS = 14 * 24 * 60 * 60 * 1000;
const FOUR_WEEKS_MS = 28 * 24 * 60 * 60 * 1000;
const ONE_MONTH_MS = 30 * 24 * 60 * 60 * 1000;

const ONE_MINUTE_MS = 60 * 1000;
const FIVE_MINUTES_MS = 5 * 60 * 1000;
const TEN_MINUTES_MS = 10 * 60 * 1000;
const THIRTY_MINUTES_MS = 30 * 60 * 1000;
const ONE_HOUR_MS = 60 * 60 * 1000;

/**
 * Generic function to downsample data from one resolution to another.
 * @param rows The input rows to condense
 * @param intervalMs The target interval (e.g., 5m or 1h)
 */
function condenseData(rows: any[], intervalMs: number): any[] {
  if (rows.length === 0) return [];

  const grouped = new Map<string, any[]>();
  
  for (const row of rows) {
    // Truncate timestamp to the target interval
    const timestamp = Math.floor(row.timestamp / intervalMs) * intervalMs;
    const key = `${row.product_id}-${timestamp}`;
    
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key)!.push(row);
  }

  const candles: any[] = [];

  for (const [key, groupRows] of grouped.entries()) {
    groupRows.sort((a, b) => a.timestamp - b.timestamp);
    
    const [productIdStr, timestampStr] = key.split('-');
    const productId = parseInt(productIdStr || '0', 10);
    const timestamp = parseInt(timestampStr || '0', 10);

    const first = groupRows[0];
    const last = groupRows[groupRows.length - 1];

    let buyHigh = -Infinity, buyLow = Infinity, buyVolSum = 0;
    let sellHigh = -Infinity, sellLow = Infinity, sellVolSum = 0;
    let buyOrdersSum = 0, sellOrdersSum = 0;
    let buyMovingWeekSum = 0, sellMovingWeekSum = 0;

    for (const r of groupRows) {
      // Handle the fact that 'prices' table has different column names than resolution tables
      const bPrice = r.buy_price !== undefined ? r.buy_price : r.buy_close;
      const sPrice = r.sell_price !== undefined ? r.sell_price : r.sell_close;
      const bVol = r.buy_volume !== undefined ? r.buy_volume : r.avg_buy_volume;
      const sVol = r.sell_volume !== undefined ? r.sell_volume : r.avg_sell_volume;
      const bOrders = r.buy_orders !== undefined ? r.buy_orders : r.avg_buy_orders;
      const sOrders = r.sell_orders !== undefined ? r.sell_orders : r.avg_sell_orders;
      const bMW = r.buy_moving_week !== undefined ? r.buy_moving_week : r.avg_buy_moving_week;
      const sMW = r.sell_moving_week !== undefined ? r.sell_moving_week : r.avg_sell_moving_week;

      if (bPrice > buyHigh) buyHigh = bPrice;
      if (bPrice < buyLow) buyLow = bPrice;
      buyVolSum += bVol;
      buyOrdersSum += bOrders;
      buyMovingWeekSum += bMW;

      if (sPrice > sellHigh) sellHigh = sPrice;
      if (sPrice < sellLow) sellLow = sPrice;
      sellVolSum += sVol;
      sellOrdersSum += sOrders;
      sellMovingWeekSum += sMW;
    }

    candles.push({
      timestamp,
      product_id: productId,
      buy_open: first.buy_price !== undefined ? first.buy_price : first.buy_open,
      buy_high: buyHigh,
      buy_low: buyLow,
      buy_close: last.buy_price !== undefined ? last.buy_price : last.buy_close,
      sell_open: first.sell_price !== undefined ? first.sell_price : first.sell_open,
      sell_high: sellHigh,
      sell_low: sellLow,
      sell_close: last.sell_price !== undefined ? last.sell_price : last.sell_close,
      avg_buy_volume: Math.floor(buyVolSum / groupRows.length),
      avg_sell_volume: Math.floor(sellVolSum / groupRows.length),
      avg_buy_orders: Math.floor(buyOrdersSum / groupRows.length),
      avg_sell_orders: Math.floor(sellOrdersSum / groupRows.length),
      avg_buy_moving_week: Math.floor(buyMovingWeekSum / groupRows.length),
      avg_sell_moving_week: Math.floor(sellMovingWeekSum / groupRows.length)
    });
  }

  return candles;
}

export function runDownsampler() {
  console.log('[Downsampler] Starting multi-tier downsampling...');

  const products = getAllProductsStmt.all() as { id: number; product_id: string }[];
  console.log(`[Downsampler] Processing ${products.length} products...`);

  for (const product of products) {
    const pId = product.id;

    // --- Tier 1: Raw (20s) -> 1-Minute (after 1 day) ---
    const tier1Cutoff = Date.now() - ONE_DAY_MS;
    const rawData = getRawPricesOlderThanForProduct(pId, tier1Cutoff);
    if (rawData.length > 0) {
      const oneMinCandles = condenseData(rawData, ONE_MINUTE_MS);
      try {
        bulkInsertOneMinPrices(oneMinCandles);
        deleteRawPricesOlderThanForProduct(pId, tier1Cutoff);
      } catch (err) {
        console.error(`[Downsampler] Tier 1 Error for ${product.product_id}:`, err);
      }
    }

    // --- Tier 2: 1-Minute -> 5-Minute (after 3 days) ---
    const tier2Cutoff = Date.now() - THREE_DAYS_MS;
    const oneMinData = getOneMinPricesOlderThanForProduct(pId, tier2Cutoff);
    if (oneMinData.length > 0) {
      const fiveMinCandles = condenseData(oneMinData, FIVE_MINUTES_MS);
      try {
        bulkInsertFiveMinPrices(fiveMinCandles);
        deleteOneMinPricesOlderThanForProduct(pId, tier2Cutoff);
      } catch (err) {
        console.error(`[Downsampler] Tier 2 Error for ${product.product_id}:`, err);
      }
    }

    // --- Tier 3: 5-Minute -> 10-Minute (after 1 week) ---
    const tier3Cutoff = Date.now() - ONE_WEEK_MS;
    const fiveMinData = getFiveMinPricesOlderThanForProduct(pId, tier3Cutoff);
    if (fiveMinData.length > 0) {
      const tenMinCandles = condenseData(fiveMinData, TEN_MINUTES_MS);
      try {
        bulkInsertTenMinPrices(tenMinCandles);
        deleteFiveMinPricesOlderThanForProduct(pId, tier3Cutoff);
      } catch (err) {
        console.error(`[Downsampler] Tier 3 Error for ${product.product_id}:`, err);
      }
    }

    // --- Tier 4: 10-Minute -> 30-Minute (after 2 weeks) ---
    const tier4Cutoff = Date.now() - TWO_WEEKS_MS;
    const tenMinData = getTenMinPricesOlderThanForProduct(pId, tier4Cutoff);
    if (tenMinData.length > 0) {
      const thirtyMinCandles = condenseData(tenMinData, THIRTY_MINUTES_MS);
      try {
        bulkInsertThirtyMinPrices(thirtyMinCandles);
        deleteTenMinPricesOlderThanForProduct(pId, tier4Cutoff);
      } catch (err) {
        console.error(`[Downsampler] Tier 4 Error for ${product.product_id}:`, err);
      }
    }

    // --- Tier 5: 30-Minute -> 1-Hour (after 4 weeks) ---
    const tier5Cutoff = Date.now() - FOUR_WEEKS_MS;
    const thirtyMinData = getThirtyMinPricesOlderThanForProduct(pId, tier5Cutoff);
    if (thirtyMinData.length > 0) {
      const hourlyCandles = condenseData(thirtyMinData, ONE_HOUR_MS);
      try {
        bulkInsertHourlyPrices(hourlyCandles);
        deleteThirtyMinPricesOlderThanForProduct(pId, tier5Cutoff);
      } catch (err) {
        console.error(`[Downsampler] Tier 5 Error for ${product.product_id}:`, err);
      }
    }
  }

  console.log('[Downsampler] All tiers complete for all products.');
  
  logHeartbeat('downsampler');
}

export function scheduleDownsampler() {
  runDownsampler();
  // Run every 24 hours
  setInterval(runDownsampler, 24 * 60 * 60 * 1000);
  
  // Log heartbeat every minute so health checks pass
  setInterval(() => logHeartbeat('downsampler'), 60000);
}

if (require.main === module) {
  scheduleDownsampler();
}
