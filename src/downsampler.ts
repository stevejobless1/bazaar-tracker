import { getRawPricesOlderThan, deleteRawPricesOlderThan, bulkInsertHourlyPrices } from './db';

const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

export function runDownsampler() {
  console.log('[Downsampler] Starting downsampling process...');
  const cutoffTime = Date.now() - SEVEN_DAYS_MS;
  
  // Get all data older than 7 days
  const oldData = getRawPricesOlderThan(cutoffTime);
  
  if (oldData.length === 0) {
    console.log('[Downsampler] No data older than 7 days found.');
    return;
  }

  console.log(`[Downsampler] Found ${oldData.length} old records to compress.`);

  // Group by (product_id, hourly_timestamp)
  const grouped = new Map<string, any[]>();
  
  for (const row of oldData) {
    // Truncate timestamp to the start of the hour
    const date = new Date(row.timestamp);
    date.setMinutes(0, 0, 0);
    const hourTimestamp = date.getTime();
    
    const key = `${row.product_id}-${hourTimestamp}`;
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key)!.push(row);
  }

  const hourlyCandles: any[] = [];

  for (const [key, rows] of grouped.entries()) {
    // Sort rows by timestamp to ensure correct open/close
    rows.sort((a, b) => a.timestamp - b.timestamp);
    
    const [productIdStr, hourTimestampStr] = key.split('-');
    const productId = parseInt(productIdStr || '0', 10);
    const hourTimestamp = parseInt(hourTimestampStr || '0', 10);

    const first = rows[0];
    const last = rows[rows.length - 1];

    let buyHigh = -Infinity, buyLow = Infinity, buyVolSum = 0;
    let sellHigh = -Infinity, sellLow = Infinity, sellVolSum = 0;
    let buyOrdersSum = 0, sellOrdersSum = 0;
    let buyMovingWeekSum = 0, sellMovingWeekSum = 0;

    for (const r of rows) {
      if (r.buy_price > buyHigh) buyHigh = r.buy_price;
      if (r.buy_price < buyLow) buyLow = r.buy_price;
      buyVolSum += r.buy_volume;
      buyOrdersSum += r.buy_orders;
      buyMovingWeekSum += r.buy_moving_week;

      if (r.sell_price > sellHigh) sellHigh = r.sell_price;
      if (r.sell_price < sellLow) sellLow = r.sell_price;
      sellVolSum += r.sell_volume;
      sellOrdersSum += r.sell_orders;
      sellMovingWeekSum += r.sell_moving_week;
    }

    hourlyCandles.push({
      timestamp: hourTimestamp,
      product_id: productId,
      buy_open: first.buy_price,
      buy_high: buyHigh,
      buy_low: buyLow,
      buy_close: last.buy_price,
      sell_open: first.sell_price,
      sell_high: sellHigh,
      sell_low: sellLow,
      sell_close: last.sell_price,
      avg_buy_volume: Math.floor(buyVolSum / rows.length),
      avg_sell_volume: Math.floor(sellVolSum / rows.length),
      avg_buy_orders: Math.floor(buyOrdersSum / rows.length),
      avg_sell_orders: Math.floor(sellOrdersSum / rows.length),
      avg_buy_moving_week: Math.floor(buyMovingWeekSum / rows.length),
      avg_sell_moving_week: Math.floor(sellMovingWeekSum / rows.length)
    });
  }

  // Insert condensed data
  try {
    bulkInsertHourlyPrices(hourlyCandles);
    console.log(`[Downsampler] Successfully condensed into ${hourlyCandles.length} hourly candles.`);
    
    // Delete the raw data to free up space
    deleteRawPricesOlderThan(cutoffTime);
    console.log(`[Downsampler] Deleted ${oldData.length} raw records.`);
  } catch (err) {
    console.error('[Downsampler] Error inserting hourly prices:', err);
  }
}

// Run once a day at midnight
function scheduleDownsampler() {
  runDownsampler(); // run once on start
  // Run every 24 hours
  setInterval(runDownsampler, 24 * 60 * 60 * 1000);
}

if (require.main === module) {
  scheduleDownsampler();
}
