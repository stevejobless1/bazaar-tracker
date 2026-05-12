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
  getHourlyPricesOlderThanForProduct,
  deleteHourlyPricesOlderThanForProduct,
  bulkInsertOneMinPrices,
  bulkInsertFiveMinPrices,
  bulkInsertTenMinPrices,
  bulkInsertThirtyMinPrices,
  bulkInsertHourlyPrices,
  bulkInsertDailyPrices,
  incrementalVacuum,
  logHeartbeat,
  cleanupHeartbeats,
  getAllProducts
} from './db';
import { notifyError, notifySuccess, notifyWarning, trackFailure, resetFailure } from './discord';

const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const TWO_WEEKS_MS = 14 * 24 * 60 * 60 * 1000;
const FOUR_WEEKS_MS = 28 * 24 * 60 * 60 * 1000;
const TWO_MONTHS_MS = 60 * 24 * 60 * 60 * 1000;

const ONE_MINUTE_MS = 60 * 1000;
const FIVE_MINUTES_MS = 5 * 60 * 1000;
const TEN_MINUTES_MS = 10 * 60 * 1000;
const THIRTY_MINUTES_MS = 30 * 60 * 1000;
const ONE_HOUR_MS = 60 * 60 * 1000;

// Run every hour instead of every 24 hours to keep data fresh
const DOWNSAMPLER_INTERVAL_MS = ONE_HOUR_MS;

/**
 * Generic function to downsample data from one resolution to another.
 * Uses a safe delimiter '|' to avoid ambiguity with product_id or timestamp values.
 * @param rows The input rows to condense
 * @param intervalMs The target interval (e.g., 5m or 1h)
 */
function condenseData(rows: any[], intervalMs: number): any[] {
  if (rows.length === 0) return [];

  const grouped = new Map<string, any[]>();
  
  for (const row of rows) {
    // Truncate timestamp to the target interval
    const bucketTs = Math.floor(row.timestamp / intervalMs) * intervalMs;
    // Use '|' delimiter to avoid any ambiguity with '-' in numbers
    const key = `${row.product_id}|${bucketTs}`;
    
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key)!.push(row);
  }

  const candles: any[] = [];

  for (const [key, groupRows] of grouped.entries()) {
    groupRows.sort((a, b) => a.timestamp - b.timestamp);
    
    // Split on '|' — safe delimiter
    const parts = key.split('|');
    if (parts.length !== 2) {
      console.error(`[Downsampler] Invalid key format: ${key}`);
      continue;
    }
    const productId = parts[0];
    const timestamp = parseInt(parts[1], 10);

    if (!productId || isNaN(timestamp)) {
      console.error(`[Downsampler] Failed to parse key: ${key}`);
      continue;
    }

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
      buyVolSum += bVol || 0;
      buyOrdersSum += bOrders || 0;
      buyMovingWeekSum += bMW || 0;

      if (sPrice > sellHigh) sellHigh = sPrice;
      if (sPrice < sellLow) sellLow = sPrice;
      sellVolSum += sVol || 0;
      sellOrdersSum += sOrders || 0;
      sellMovingWeekSum += sMW || 0;
    }

    // Guard against Infinity values (no valid data in group)
    if (buyHigh === -Infinity) buyHigh = 0;
    if (buyLow === Infinity) buyLow = 0;
    if (sellHigh === -Infinity) sellHigh = 0;
    if (sellLow === Infinity) sellLow = 0;

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
  const startTime = Date.now();
  console.log('[Downsampler] Starting multi-tier downsampling...');

  let products: { id: string; product_id: string }[];
  try {
    products = getAllProducts() as { id: string; product_id: string }[];
  } catch (err) {
    const msg = `Failed to query products: ${(err as Error).message}`;
    console.error(`[Downsampler] ${msg}`);
    notifyError('downsampler', 'Downsampler Failed to Start', msg);
    return;
  }

  console.log(`[Downsampler] Processing ${products.length} products...`);

  let totalProcessed = 0;
  let totalErrors = 0;
  let tierStats = { t1: 0, t2: 0, t3: 0, t4: 0, t5: 0, t6: 0 };

  for (const product of products) {
    const pId = product.product_id;

    try {
      // --- Tier 1: Raw (20s) -> 1-Minute (after 1 day) ---
      const tier1Cutoff = Date.now() - ONE_DAY_MS;
      const rawData = getRawPricesOlderThanForProduct(pId, tier1Cutoff);
      if (rawData.length > 0) {
        const oneMinCandles = condenseData(rawData, ONE_MINUTE_MS);
        bulkInsertOneMinPrices(oneMinCandles);
        deleteRawPricesOlderThanForProduct(pId, tier1Cutoff);
        tierStats.t1 += rawData.length;
      }

      // --- Tier 2: 1-Minute -> 5-Minute (after 3 days) ---
      const tier2Cutoff = Date.now() - THREE_DAYS_MS;
      const oneMinData = getOneMinPricesOlderThanForProduct(pId, tier2Cutoff);
      if (oneMinData.length > 0) {
        const fiveMinCandles = condenseData(oneMinData, FIVE_MINUTES_MS);
        bulkInsertFiveMinPrices(fiveMinCandles);
        deleteOneMinPricesOlderThanForProduct(pId, tier2Cutoff);
        tierStats.t2 += oneMinData.length;
      }

      // --- Tier 3: 5-Minute -> 10-Minute (after 1 week) ---
      const tier3Cutoff = Date.now() - ONE_WEEK_MS;
      const fiveMinData = getFiveMinPricesOlderThanForProduct(pId, tier3Cutoff);
      if (fiveMinData.length > 0) {
        const tenMinCandles = condenseData(fiveMinData, TEN_MINUTES_MS);
        bulkInsertTenMinPrices(tenMinCandles);
        deleteFiveMinPricesOlderThanForProduct(pId, tier3Cutoff);
        tierStats.t3 += fiveMinData.length;
      }

      // --- Tier 4: 10-Minute -> 30-Minute (after 2 weeks) ---
      const tier4Cutoff = Date.now() - TWO_WEEKS_MS;
      const tenMinData = getTenMinPricesOlderThanForProduct(pId, tier4Cutoff);
      if (tenMinData.length > 0) {
        const thirtyMinCandles = condenseData(tenMinData, THIRTY_MINUTES_MS);
        bulkInsertThirtyMinPrices(thirtyMinCandles);
        deleteTenMinPricesOlderThanForProduct(pId, tier4Cutoff);
        tierStats.t4 += tenMinData.length;
      }

      // --- Tier 5: 30-Minute -> 1-Hour (after 4 weeks) ---
      const tier5Cutoff = Date.now() - FOUR_WEEKS_MS;
      const thirtyMinData = getThirtyMinPricesOlderThanForProduct(pId, tier5Cutoff);
      if (thirtyMinData.length > 0) {
        const hourlyCandles = condenseData(thirtyMinData, ONE_HOUR_MS);
        bulkInsertHourlyPrices(hourlyCandles);
        deleteThirtyMinPricesOlderThanForProduct(pId, tier5Cutoff);
        tierStats.t5 += thirtyMinData.length;
      }

      // --- Tier 6: 1-Hour -> 1-Day (after 2 months) ---
      const tier6Cutoff = Date.now() - TWO_MONTHS_MS;
      const hourlyData = getHourlyPricesOlderThanForProduct(pId, tier6Cutoff);
      if (hourlyData.length > 0) {
        const dailyCandles = condenseData(hourlyData, ONE_DAY_MS);
        bulkInsertDailyPrices(dailyCandles);
        deleteHourlyPricesOlderThanForProduct(pId, tier6Cutoff);
        tierStats.t6 += hourlyData.length;
      }

      totalProcessed++;
    } catch (err) {
      totalErrors++;
      const msg = `Downsampling error for ${product.product_id}: ${(err as Error).message}`;
      console.error(`[Downsampler] ${msg}`);
      
      // Only notify Discord on first few errors to avoid spam
      if (totalErrors <= 3) {
        notifyWarning('downsampler', 'Downsampler Product Error', msg);
      }
    }
  }

  const elapsed = Date.now() - startTime;
  const summary = `Processed ${totalProcessed}/${products.length} products in ${(elapsed / 1000).toFixed(1)}s | Errors: ${totalErrors}`;
  console.log(`[Downsampler] ${summary}`);
  console.log(`[Downsampler] Tier stats — Raw→1m: ${tierStats.t1}, 1m→5m: ${tierStats.t2}, 5m→10m: ${tierStats.t3}, 10m→30m: ${tierStats.t4}, 30m→1h: ${tierStats.t5}, 1h→1d: ${tierStats.t6}`);

  if (totalErrors > 0) {
    notifyWarning('downsampler', 'Downsampler Completed with Errors', summary, [
      { name: 'Products OK', value: `${totalProcessed}`, inline: true },
      { name: 'Errors', value: `${totalErrors}`, inline: true },
      { name: 'Duration', value: `${(elapsed / 1000).toFixed(1)}s`, inline: true },
    ]);
  }

  // Run maintenance tasks
  console.log('[Downsampler] Running maintenance tasks...');
  try {
    cleanupHeartbeats();
  } catch (err) {
    console.error('[Downsampler] Heartbeat cleanup error:', err);
  }

  try {
    incrementalVacuum();
  } catch (err) {
    console.error('[Downsampler] Incremental vacuum error:', err);
  }

  logHeartbeat('downsampler');
}

export function scheduleDownsampler() {
  console.log(`[Downsampler] Scheduled to run every ${DOWNSAMPLER_INTERVAL_MS / 60000} minutes.`);
  
  // Notify startup
  notifySuccess('downsampler', 'Downsampler Started', `Running every ${DOWNSAMPLER_INTERVAL_MS / 60000} minutes.`);

  // Run the first time after a short delay (let the DB initialize)
  setTimeout(() => {
    runDownsampler();
  }, 5000);

  // Run every hour
  setInterval(() => {
    try {
      runDownsampler();
    } catch (err) {
      const msg = `Unhandled downsampler error: ${(err as Error).message}`;
      console.error(`[Downsampler] ${msg}`);
      notifyError('downsampler', 'Downsampler Crash', msg);
    }
  }, DOWNSAMPLER_INTERVAL_MS);
  
  // Log heartbeat every minute so health checks pass
  setInterval(() => logHeartbeat('downsampler'), 60000);
}

if (require.main === module) {
  scheduleDownsampler();
}
