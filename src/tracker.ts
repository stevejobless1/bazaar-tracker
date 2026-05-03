import axios from 'axios';
import { 
  loadAllProductsIntoCache, 
  getLastRecordedPrices, 
  bulkInsertPrices, 
  bulkUpsertLiveOrders,
  logHeartbeat,
  insertMayor,
  getLastMayor,
  LiveOrderSummaries,
  ProductPrice 
} from './db';

const BAZAAR_API_URL = 'https://api.hypixel.net/v2/skyblock/bazaar';
const MAYOR_API_URL = 'https://api.hypixel.net/v2/resources/skyblock/election';
const POLL_INTERVAL_MS = 20 * 1000; // 20 seconds
const MAYOR_POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

const HYPIXEL_API_KEY = process.env.HYPIXEL_API_KEY;

// Keep track of the last known state to compute deltas
let lastState = new Map<string, ProductPrice>();

async function fetchBazaarData() {
  try {
    const headers: any = { timeout: 10000 };
    if (HYPIXEL_API_KEY) {
      headers.headers = { 'API-Key': HYPIXEL_API_KEY };
    }
    const response = await axios.get(BAZAAR_API_URL, headers);
    return response.data;
  } catch (error) {
    console.error(`[Tracker] Failed to fetch Bazaar API: ${(error as Error).message}`);
    return null;
  }
}

async function fetchMayorData() {
  try {
    const headers: any = { timeout: 10000 };
    if (HYPIXEL_API_KEY) {
      headers.headers = { 'API-Key': HYPIXEL_API_KEY };
    }
    const response = await axios.get(MAYOR_API_URL, headers);
    const data = response.data;
    if (data && data.success && data.mayor) {
      const currentMayorName = data.mayor.name;
      const lastMayor = getLastMayor();
      
      if (!lastMayor || lastMayor.name !== currentMayorName) {
        console.log(`[Tracker] Mayor changed from ${lastMayor?.name || 'Unknown'} to ${currentMayorName}`);
        insertMayor(currentMayorName, Date.now(), Date.now() + 450000000); // ~5.2 days for term end
      }
    }
  } catch (error) {
    console.error(`[Tracker] Failed to fetch Mayor API: ${(error as Error).message}`);
  }
}

async function runTracker() {
  console.log('[Tracker] Starting Bazaar tracker loop...');
  
  // Pre-load DB cache
  loadAllProductsIntoCache();
  
  // Load the most recent prices from the DB to seed our delta map
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

  const tick = async () => {
    const data = await fetchBazaarData();
    if (!data || !data.success || !data.products) return;

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
      
      // Always extract live orders so the website has the freshest order book possible
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
        console.log(`[Tracker] Saved ${changedProducts}/${totalProducts} items (Delta: -${totalProducts - changedProducts}) at ${new Date(timestamp).toISOString()}`);
      } catch (err) {
        console.error('[Tracker] DB Insert Error:', err);
      }
    } else {
      console.log(`[Tracker] No changes detected at ${new Date(timestamp).toISOString()}`);
    }

    // Upsert the live order summaries
    if (liveOrdersToInsert.length > 0) {
      try {
        bulkUpsertLiveOrders(liveOrdersToInsert);
      } catch (err) {
        console.error('[Tracker] DB Live Orders Insert Error:', err);
      }
    }

    // Log heartbeat for status page
    logHeartbeat('tracker');
  };

  // Run immediately, then interval
  await tick();
  setInterval(tick, POLL_INTERVAL_MS);

  // Run mayor check immediately, then interval
  await fetchMayorData();
  setInterval(fetchMayorData, MAYOR_POLL_INTERVAL_MS);
}

runTracker();
