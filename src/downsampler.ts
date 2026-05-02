import { 
  getRawPricesOlderThan, 
  deleteRawPricesOlderThan, 
  getOneMinPricesOlderThan,
  deleteOneMinPricesOlderThan,
  getFiveMinPricesOlderThan,
  deleteFiveMinPricesOlderThan,
  getThirtyMinPricesOlderThan,
  deleteThirtyMinPricesOlderThan,
  bulkInsertOneMinPrices,
  bulkInsertFiveMinPrices,
  bulkInsertThirtyMinPrices,
  bulkInsertHourlyPrices,
  vacuumDB,
  logHeartbeat
} from './db';

const TWENTY_FOUR_HOURS_MS = 24 * 60 * 60 * 1000;
const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;
const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;

const ONE_MINUTE_MS = 60 * 1000;
const FIVE_MINUTES_MS = 5 * 60 * 1000;
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

  // --- Tier 1: Raw (20s) -> 1-Minute (after 24h) ---
  const tier1Cutoff = Date.now() - TWENTY_FOUR_HOURS_MS;
  const rawData = getRawPricesOlderThan(tier1Cutoff);
  if (rawData.length > 0) {
    console.log(`[Downsampler] Tier 1: Condensing ${rawData.length} raw records into 1m candles...`);
    const oneMinCandles = condenseData(rawData, ONE_MINUTE_MS);
    try {
      bulkInsertOneMinPrices(oneMinCandles);
      deleteRawPricesOlderThan(tier1Cutoff);
      console.log(`[Downsampler] Tier 1 Complete: Created ${oneMinCandles.length} 1m candles.`);
    } catch (err) {
      console.error('[Downsampler] Tier 1 Error:', err);
    }
  }

  // --- Tier 2: 1-Minute -> 5-Minute (after 3 days) ---
  const tier2Cutoff = Date.now() - THREE_DAYS_MS;
  const oneMinData = getOneMinPricesOlderThan(tier2Cutoff);
  if (oneMinData.length > 0) {
    console.log(`[Downsampler] Tier 2: Condensing ${oneMinData.length} 1m records into 5m candles...`);
    const fiveMinCandles = condenseData(oneMinData, FIVE_MINUTES_MS);
    try {
      bulkInsertFiveMinPrices(fiveMinCandles);
      deleteOneMinPricesOlderThan(tier2Cutoff);
      console.log(`[Downsampler] Tier 2 Complete: Created ${fiveMinCandles.length} 5m candles.`);
    } catch (err) {
      console.error('[Downsampler] Tier 2 Error:', err);
    }
  }

  // --- Tier 3: 5-Minute -> 30-Minute (after 7 days) ---
  const tier3Cutoff = Date.now() - SEVEN_DAYS_MS;
  const fiveMinData = getFiveMinPricesOlderThan(tier3Cutoff);
  if (fiveMinData.length > 0) {
    console.log(`[Downsampler] Tier 3: Condensing ${fiveMinData.length} 5m records into 30m candles...`);
    const thirtyMinCandles = condenseData(fiveMinData, THIRTY_MINUTES_MS);
    try {
      bulkInsertThirtyMinPrices(thirtyMinCandles);
      deleteFiveMinPricesOlderThan(tier3Cutoff);
      console.log(`[Downsampler] Tier 3 Complete: Created ${thirtyMinCandles.length} 30m candles.`);
    } catch (err) {
      console.error('[Downsampler] Tier 3 Error:', err);
    }
  }

  // --- Tier 4: 30-Minute -> 1-Hour (after 14 days) ---
  const tier4Cutoff = Date.now() - FOURTEEN_DAYS_MS;
  const thirtyMinData = getThirtyMinPricesOlderThan(tier4Cutoff);
  if (thirtyMinData.length > 0) {
    console.log(`[Downsampler] Tier 4: Condensing ${thirtyMinData.length} 30m records into 1h candles...`);
    const hourlyCandles = condenseData(thirtyMinData, ONE_HOUR_MS);
    try {
      bulkInsertHourlyPrices(hourlyCandles);
      deleteThirtyMinPricesOlderThan(tier4Cutoff);
      console.log(`[Downsampler] Tier 4 Complete: Created ${hourlyCandles.length} 1h candles.`);
    } catch (err) {
      console.error('[Downsampler] Tier 4 Error:', err);
    }
  }

  // Optimize database after cleaning up
  vacuumDB();
  logHeartbeat('downsampler');
}

export function scheduleDownsampler() {
  runDownsampler();
  // Run every 24 hours
  setInterval(runDownsampler, 24 * 60 * 60 * 1000);
}

if (require.main === module) {
  scheduleDownsampler();
}
