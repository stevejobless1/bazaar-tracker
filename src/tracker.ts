import axios from 'axios';
import { 
  loadAllProductsIntoCache, 
  getLastRecordedPrices, 
  bulkInsertPrices, 
  bulkUpsertLiveOrders,
  logHeartbeat,
  insertMayor,
  getLastMayor,
  insertVolumeDelta,
  LiveOrderSummaries,
  ProductPrice,
  getDatabaseSize,
  db
} from './db';

import { seedLegacyMayors } from './seed';
import { notifyError, notifySuccess, notifyWarning, notifyInfo, trackFailure, resetFailure } from './discord';

const BAZAAR_API_URL = 'https://api.hypixel.net/v2/skyblock/bazaar';
const MAYOR_API_URL = 'https://api.hypixel.net/v2/resources/skyblock/election';
const POLL_INTERVAL_MS = 20 * 1000; // 20 seconds
const MAYOR_POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const MAX_CONSECUTIVE_FAILURES = 10; // Alert after this many consecutive API failures
const STATUS_REPORT_INTERVAL_MS = 60 * 60 * 1000; // Hourly status report

const HYPIXEL_API_KEY = process.env.HYPIXEL_API_KEY;

// Keep track of the last known state to compute deltas
let lastState = new Map<string, ProductPrice>();
let totalTicks = 0;
let totalInserts = 0;
let totalApiErrors = 0;
let totalDbErrors = 0;
let startupTime = Date.now();

async function fetchBazaarData(): Promise<any | null> {
  try {
    const config: any = { timeout: 15000 };
    if (HYPIXEL_API_KEY) {
      config.headers = { 'API-Key': HYPIXEL_API_KEY };
    }
    const response = await axios.get(BAZAAR_API_URL, config);
    return response.data;
  } catch (error) {
    const msg = (error as Error).message;
    console.error(`[Tracker] Failed to fetch Bazaar API: ${msg}`);
    
    const failures = trackFailure('tracker', 'api_fetch');
    if (failures === MAX_CONSECUTIVE_FAILURES) {
      notifyError('tracker', 'Bazaar API Down', 
        `Failed to fetch Bazaar API ${MAX_CONSECUTIVE_FAILURES} consecutive times.\nLast error: ${msg}`,
        [{ name: 'Total API Errors', value: `${totalApiErrors}`, inline: true }]
      );
    } else if (failures > MAX_CONSECUTIVE_FAILURES && failures % 50 === 0) {
      // Reminder every 50 failures after the first alert
      notifyWarning('tracker', 'Bazaar API Still Down', 
        `${failures} consecutive failures. Last error: ${msg}`
      );
    }
    
    totalApiErrors++;
    return null;
  }
}

async function fetchMayorData() {
  try {
    const config: any = { timeout: 15000 };
    if (HYPIXEL_API_KEY) {
      config.headers = { 'API-Key': HYPIXEL_API_KEY };
    }
    const response = await axios.get(MAYOR_API_URL, config);
    const data = response.data;
    if (data && data.success && data.mayor) {
      const currentMayorName = data.mayor.name;
      const lastMayor = getLastMayor();
      
      if (!lastMayor || lastMayor.name !== currentMayorName) {
        console.log(`[Tracker] Mayor changed from ${lastMayor?.name || 'Unknown'} to ${currentMayorName}`);
        insertMayor(currentMayorName, Date.now(), Date.now() + 450000000); // ~5.2 days for term end
        
        notifyInfo('tracker', 'Mayor Changed', 
          `New mayor: **${currentMayorName}** (was ${lastMayor?.name || 'Unknown'})`,
          [{ name: 'Mayor', value: currentMayorName, inline: true }]
        );
      }
    }
  } catch (error) {
    console.error(`[Tracker] Failed to fetch Mayor API: ${(error as Error).message}`);
  }
}

function getDbDiagnostics(): string {
  try {
    const counts = {
      raw: (db.prepare('SELECT COUNT(*) as cnt FROM prices').get() as any)?.cnt || 0,
      '1m': (db.prepare('SELECT COUNT(*) as cnt FROM one_min_prices').get() as any)?.cnt || 0,
      '5m': (db.prepare('SELECT COUNT(*) as cnt FROM five_min_prices').get() as any)?.cnt || 0,
      '10m': (db.prepare('SELECT COUNT(*) as cnt FROM ten_min_prices').get() as any)?.cnt || 0,
      '30m': (db.prepare('SELECT COUNT(*) as cnt FROM thirty_min_prices').get() as any)?.cnt || 0,
      '1h': (db.prepare('SELECT COUNT(*) as cnt FROM hourly_prices').get() as any)?.cnt || 0,
      '1d': (db.prepare('SELECT COUNT(*) as cnt FROM daily_prices').get() as any)?.cnt || 0,
    };
    
    const pageCount = (db.prepare('PRAGMA page_count').get() as any)?.page_count || 0;
    const pageSize = (db.prepare('PRAGMA page_size').get() as any)?.page_size || 0;
    const dbSizeMB = ((pageCount * pageSize) / (1024 * 1024)).toFixed(1);
    
    return `Storage: ${dbSizeMB} MB | Rows: Raw=${counts.raw.toLocaleString()}, 1m=${counts['1m'].toLocaleString()}, 5m=${counts['5m'].toLocaleString()}, 10m=${counts['10m'].toLocaleString()}, 30m=${counts['30m'].toLocaleString()}, 1h=${counts['1h'].toLocaleString()}, 1d=${counts['1d'].toLocaleString()}`;
  } catch (err) {
    return `Unable to read diagnostics: ${(err as Error).message}`;
  }
}

async function runTracker() {
  console.log('[Tracker] Starting Bazaar tracker loop...');
  
  // Pre-load DB cache
  try {
    loadAllProductsIntoCache();
  } catch (err) {
    const msg = `Failed to load product cache: ${(err as Error).message}`;
    console.error(`[Tracker] ${msg}`);
    notifyError('tracker', 'Tracker Startup Failed', msg);
    // Don't exit — the cache will be built as products come in
  }
  
  // Load the most recent prices from the DB to seed our delta map
  try {
    const dbLastPrices = getLastRecordedPrices();
    for (const [productIdStr, priceObj] of dbLastPrices.entries()) {
      lastState.set(productIdStr, {
        productId: productIdStr,
        timestamp: priceObj.timestamp,
        buyPrice: priceObj.buy_price,
        sellPrice: priceObj.sell_price,
        buyVolume: priceObj.buy_volume,
        sellVolume: priceObj.sell_volume,
        buyOrders: priceObj.buy_orders,
        sellOrders: priceObj.sell_orders,
        buyMovingWeek: priceObj.buy_moving_week,
        sellMovingWeek: priceObj.sell_moving_week
      });
    }
    console.log(`[Tracker] Loaded ${lastState.size} products from DB state.`);
  } catch (err) {
    console.error(`[Tracker] Failed to load last state: ${(err as Error).message}`);
    // Non-fatal: we'll just not have delta comparison for the first tick
  }

  const tick = async () => {
    totalTicks++;
    
    const data = await fetchBazaarData();
    if (!data || !data.success || !data.products) return;

    // API succeeded — reset failure counter
    resetFailure('tracker', 'api_fetch');

    const timestamp = data.lastUpdated || Date.now();
    const toInsert: ProductPrice[] = [];
    const liveOrdersToInsert: LiveOrderSummaries[] = [];

    let totalProducts = 0;
    let changedProducts = 0;

    for (const [productId, productData] of Object.entries(data.products)) {
      const q = (productData as any).quick_status;
      if (!q) continue;

      totalProducts++;

      const currentBuyPrice = q.buyPrice || 0;
      const currentSellPrice = q.sellPrice || 0;
      const currentBuyVolume = q.buyVolume || 0;
      const currentSellVolume = q.sellVolume || 0;
      const currentBuyOrders = q.buyOrders || 0;
      const currentSellOrders = q.sellOrders || 0;
      const currentBuyMovingWeek = q.buyMovingWeek || 0;
      const currentSellMovingWeek = q.sellMovingWeek || 0;

      const previous = lastState.get(productId);

      // Volume Delta Logic (Preparing for hour-by-hour tracking)
      let buyVolumeDelta = 0;
      let sellVolumeDelta = 0;
      if (previous) {
        buyVolumeDelta = Math.max(0, currentBuyMovingWeek - previous.buyMovingWeek);
        sellVolumeDelta = Math.max(0, currentSellMovingWeek - previous.sellMovingWeek);
        
        if (buyVolumeDelta > 0 || sellVolumeDelta > 0) {
          try {
            insertVolumeDelta(productId, timestamp, buyVolumeDelta, sellVolumeDelta);
          } catch (err) {
            // Non-critical — don't crash the tick for volume tracking
          }
        }
      }

      // Delta logic: Only save if any of the core indicators change
      let hasChanged = true;
      if (previous) {
        if (
          previous.buyPrice === currentBuyPrice &&
          previous.sellPrice === currentSellPrice &&
          previous.buyVolume === currentBuyVolume &&
          previous.sellVolume === currentSellVolume &&
          previous.buyOrders === currentBuyOrders &&
          previous.sellOrders === currentSellOrders &&
          previous.buyMovingWeek === currentBuyMovingWeek &&
          previous.sellMovingWeek === currentSellMovingWeek
        ) {
          hasChanged = false;
        }
      }

      if (hasChanged) {
        changedProducts++;
        const newRecord: ProductPrice = {
          timestamp,
          productId,
          buyPrice: currentBuyPrice,
          sellPrice: currentSellPrice,
          buyVolume: currentBuyVolume,
          sellVolume: currentSellVolume,
          buyOrders: currentBuyOrders,
          sellOrders: currentSellOrders,
          buyMovingWeek: currentBuyMovingWeek,
          sellMovingWeek: currentSellMovingWeek
        };
        toInsert.push(newRecord);
        lastState.set(productId, newRecord);
      }
      
      // Always extract live orders
      const buySummary = (productData as any).buy_summary || [];
      const sellSummary = (productData as any).sell_summary || [];
      liveOrdersToInsert.push({
        productId,
        buySummary: JSON.stringify(buySummary),
        sellSummary: JSON.stringify(sellSummary)
      });
    }

    if (toInsert.length > 0) {
      try {
        bulkInsertPrices(toInsert);
        totalInserts += toInsert.length;
        resetFailure('tracker', 'db_insert');
        console.log(`[Tracker] Saved ${changedProducts}/${totalProducts} items (Delta: -${totalProducts - changedProducts}) at ${new Date(timestamp).toISOString()}`);
      } catch (err) {
        totalDbErrors++;
        const failures = trackFailure('tracker', 'db_insert');
        const msg = `DB Insert Error: ${(err as Error).message}`;
        console.error(`[Tracker] ${msg}`);
        
        if (failures === 5) {
          notifyError('tracker', 'Database Write Failure', 
            `Failed to write prices ${failures} consecutive times.\n${msg}\n\n${getDbDiagnostics()}`
          );
        }
      }
    } else {
      console.log(`[Tracker] No changes detected at ${new Date(timestamp).toISOString()}`);
    }

    // Upsert the live order summaries
    if (liveOrdersToInsert.length > 0) {
      try {
        bulkUpsertLiveOrders(liveOrdersToInsert);
      } catch (err) {
        console.error('[Tracker] DB Live Orders Insert Error:', (err as Error).message);
      }
    }

    // Log heartbeat for status page
    logHeartbeat('tracker');
  };

  // Seed legacy mayors if needed
  try {
    await seedLegacyMayors();
  } catch (err) {
    console.error('[Tracker] Mayor seeding error:', (err as Error).message);
  }

  // Notify successful startup
  const dbSize = getDatabaseSize();
  notifySuccess('tracker', 'Tracker Started', 
    `Polling Hypixel Bazaar API every ${POLL_INTERVAL_MS / 1000}s.\nDatabase storage: **${dbSize.sizeMB} MB**`,
    [
      { name: 'Products Loaded', value: `${lastState.size}`, inline: true },
      { name: 'API Key', value: HYPIXEL_API_KEY ? 'Configured' : 'Not Set', inline: true },
      { name: 'Diagnostics', value: `\`${getDbDiagnostics().split('|')[1].trim()}\``, inline: false },
    ]
  );

  // Run immediately, then interval
  await tick();
  setInterval(async () => {
    try {
      await tick();
    } catch (err) {
      const msg = `Unhandled tick error: ${(err as Error).message}`;
      console.error(`[Tracker] ${msg}`);
      notifyError('tracker', 'Tracker Tick Crash', msg);
    }
  }, POLL_INTERVAL_MS);

  // Run mayor check immediately, then interval
  await fetchMayorData();
  setInterval(fetchMayorData, MAYOR_POLL_INTERVAL_MS);

  // Periodic status report (every hour)
  setInterval(() => {
    const uptimeMs = Date.now() - startupTime;
    const uptimeHours = (uptimeMs / (1000 * 60 * 60)).toFixed(1);
    
    const dbSize = getDatabaseSize();
    notifyInfo('tracker', 'Hourly Status Report', 
      `Tracker has been running for **${uptimeHours}h**.\nDatabase storage: **${dbSize.sizeMB} MB**`,
      [
        { name: 'Total Ticks', value: `${totalTicks}`, inline: true },
        { name: 'Total Inserts', value: `${totalInserts.toLocaleString()}`, inline: true },
        { name: 'API Errors', value: `${totalApiErrors}`, inline: true },
        { name: 'DB Errors', value: `${totalDbErrors}`, inline: true },
        { name: 'Products Tracked', value: `${lastState.size}`, inline: true },
        { name: 'Row Stats', value: `\`${getDbDiagnostics().split('|')[1].trim()}\``, inline: false },
      ]
    );
  }, STATUS_REPORT_INTERVAL_MS);
}

// Global error handlers
process.on('uncaughtException', (err) => {
  console.error('[Tracker] UNCAUGHT EXCEPTION:', err);
  notifyError('tracker', 'Uncaught Exception', `\`\`\`\n${err.stack || err.message}\n\`\`\``);
  // Don't exit — let the tracker try to continue
});

process.on('unhandledRejection', (reason) => {
  console.error('[Tracker] UNHANDLED REJECTION:', reason);
  notifyError('tracker', 'Unhandled Rejection', `\`\`\`\n${String(reason)}\n\`\`\``);
});

runTracker();
