import axios from 'axios';
import { 
  initDB, 
  bulkInsertPrices, 
  loadAllProductsIntoCache, 
  getLastRecordedPrices,
  logHeartbeat
} from './db';
import { notifyError, notifyWarning, notifyInfo, trackFailure, resetFailure } from './discord';

// Configuration
const BAZAAR_API_URL = 'https://api.hypixel.net/v2/skyblock/bazaar';
const POLL_INTERVAL_MS = 20 * 1000; // 20 seconds
const MAX_CONSECUTIVE_FAILURES = 10; // Alert after this many consecutive API failures
const STATUS_REPORT_INTERVAL_MS = 60 * 60 * 1000; // Hourly status report

// Clean up API Key (strip potential quotes from Coolify env UI)
const HYPIXEL_API_KEY = process.env.HYPIXEL_API_KEY?.replace(/['"]/g, '').trim();

// Keep track of the last known state to compute deltas
let lastState = new Map<string, ProductPrice>();

interface ProductPrice {
  productId: string;
  timestamp: number;
  buyPrice: number;
  sellPrice: number;
  buyVolume: number;
  sellVolume: number;
  buyOrders: number;
  sellOrders: number;
  buyMovingWeek: number;
  sellMovingWeek: number;
}

// Stats for reporting
let totalTicks = 0;
let totalApiErrors = 0;
let totalItemsSaved = 0;
let startTime = Date.now();

async function fetchBazaarData() {
  try {
    const config = {
      headers: {
        'API-Key': HYPIXEL_API_KEY
      },
      timeout: 10000
    };
    const response = await axios.get(BAZAAR_API_URL, config);
    return response.data;
  } catch (error) {
    const msg = (error as Error).message;
    let details = '';
    
    if (axios.isAxiosError(error) && error.response) {
      details = ` | Status: ${error.response.status} | Data: ${JSON.stringify(error.response.data)}`;
    }
    
    console.error(`[Tracker] ❌ Failed to fetch Bazaar API: ${msg}${details}`);
    
    const failures = trackFailure('tracker', 'api_fetch');
    if (failures === MAX_CONSECUTIVE_FAILURES) {
      notifyError('tracker', 'Bazaar API Down', 
        `Failed to fetch Bazaar API ${MAX_CONSECUTIVE_FAILURES} consecutive times.\n**Error:** ${msg}\n${details ? `\`\`\`json\n${details}\n\`\`\`` : ''}`,
        [{ name: 'Total API Errors', value: `${totalApiErrors}`, inline: true }]
      );
    } else if (failures > MAX_CONSECUTIVE_FAILURES && failures % 50 === 0) {
      // Reminder every 50 failures after the first alert
      notifyWarning('tracker', 'Bazaar API Still Down', 
        `${failures} consecutive failures. Last error: ${msg}${details}`
      );
    }
    
    totalApiErrors++;
    return null;
  }
}

function processBazaarData(data: any): ProductPrice[] {
  const timestamp = data.lastUpdated;
  const products = data.products;
  const pricesToInsert: ProductPrice[] = [];

  for (const productId in products) {
    const p = products[productId];
    const quickStatus = p.quick_status;
    
    if (!quickStatus) continue;

    const current: ProductPrice = {
      productId,
      timestamp,
      buyPrice: quickStatus.buyPrice || 0,
      sellPrice: quickStatus.sellPrice || 0,
      buyVolume: quickStatus.buyVolume || 0,
      sellVolume: quickStatus.sellVolume || 0,
      buyOrders: quickStatus.buyOrders || 0,
      sellOrders: quickStatus.sellOrders || 0,
      buyMovingWeek: quickStatus.buyMovingWeek || 0,
      sellMovingWeek: quickStatus.sellMovingWeek || 0
    };

    // Only record if prices have changed or if it's the first time we see the product
    const prev = lastState.get(productId);
    const hasChanged = !prev || 
                       prev.buyPrice !== current.buyPrice || 
                       prev.sellPrice !== current.sellPrice ||
                       prev.buyVolume !== current.buyVolume ||
                       prev.sellVolume !== current.sellVolume;

    if (hasChanged) {
      pricesToInsert.push(current);
      lastState.set(productId, current);
    }
  }

  return pricesToInsert;
}

async function runTracker() {
    console.log(`[Tracker] 🚀 Starting Bazaar tracker loop...`);
    console.log(`[Tracker] 🔑 API Key: ${HYPIXEL_API_KEY ? `${HYPIXEL_API_KEY.substring(0, 4)}...${HYPIXEL_API_KEY.slice(-4)}` : 'MISSING!'}`);
    
    // Pre-load DB cache
    try {
      loadAllProductsIntoCache();
    } catch (err) {
      const msg = `Failed to load product cache: ${(err as Error).message}`;
      console.error(`[Tracker] ❌ ${msg}`);
      notifyError('tracker', 'Tracker Startup Failed', msg);
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
      console.log(`[Tracker] 📦 Loaded ${lastState.size} products from DB state.`);
    } catch (err) {
      console.error(`[Tracker] ❌ Failed to load last state: ${(err as Error).message}`);
    }

  const tick = async () => {
    // Log heartbeat at the VERY start of the tick
    logHeartbeat('tracker');

    const tickStartTime = Date.now();
    totalTicks++;
    
    const data = await fetchBazaarData();
    if (!data || !data.success || !data.products) return;

    // Reset failure counter on success
    resetFailure('tracker', 'api_fetch');

    const pricesToInsert = processBazaarData(data);

    if (pricesToInsert.length > 0) {
      try {
        bulkInsertPrices(pricesToInsert);
        totalItemsSaved += pricesToInsert.length;
        
        if (totalTicks % 3 === 0) {
          const duration = Date.now() - tickStartTime;
          console.log(`[Tracker] Saved ${pricesToInsert.length} changed items (Tick #${totalTicks}, ${duration}ms)`);
        }
      } catch (err) {
        console.error(`[Tracker] Failed to insert prices: ${(err as Error).message}`);
        notifyError('tracker', 'DB Write Error', `Failed to insert ${pricesToInsert.length} prices: ${(err as Error).message}`);
      }
    } else {
      if (totalTicks % 3 === 0) {
        console.log(`[Tracker] No changes detected (Tick #${totalTicks})`);
      }
    }

    // Periodic status report (e.g., hourly)
    if (Date.now() - startTime >= STATUS_REPORT_INTERVAL_MS) {
      const uptimeHr = Math.round((Date.now() - startTime) / (1000 * 60 * 60));
      notifyInfo('tracker', 'Hourly Status Report', 
        `Uptime: ${uptimeHr}h\nTicks: ${totalTicks}\nAPI Errors: ${totalApiErrors}\nItems Saved: ${totalItemsSaved}`,
        [
          { name: 'Avg Items/Tick', value: `${Math.round(totalItemsSaved / totalTicks)}`, inline: true },
          { name: 'Tick Rate', value: `${Math.round(totalTicks / (uptimeHr || 1))} / hr`, inline: true }
        ]
      );
      // Reset period stats
      startTime = Date.now();
      totalTicks = 0;
      totalItemsSaved = 0;
      totalApiErrors = 0;
    }
  };

  // Run immediately then on interval
  await tick();
  setInterval(tick, POLL_INTERVAL_MS);
}

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Tracker] SIGTERM received. Cleaning up...');
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  console.error('[Tracker] UNCAUGHT EXCEPTION:', err);
  notifyError('tracker', 'Uncaught Exception', `\`\`\`\n${err.stack || err.message}\n\`\`\``);
  // Give Discord time to send before exiting
  setTimeout(() => process.exit(1), 2000);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[Tracker] UNHANDLED REJECTION:', reason);
  notifyError('tracker', 'Unhandled Rejection', `Reason: ${reason}`);
});

runTracker();
