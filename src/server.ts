import express from 'express';
import cors from 'cors';
import axios from 'axios';
import compression from 'compression';
import { 
  getLastRecordedPrices, 
  getRecentHistory, 
  getOneMinHistory,
  getFiveMinHistory, 
  getTenMinHistory,
  getThirtyMinHistory,
  getHourlyHistory,
  getDailyHistory,
  getUnifiedHistory,
  getLiveOrders, 
  getStatusStats,
  getMayorsInRange,
  getVolumeHistory,
  logHeartbeat,
  db
} from './db';

const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());

// Simple auth middleware
const AUTH_PASSWORD = process.env.DASHBOARD_PASSWORD || 'fusion';
const authMiddleware = (req: any, res: any, next: any) => {
  const authCookie = req.headers.cookie?.split(';').find((c: string) => c.trim().startsWith('bt_auth='));
  if (authCookie && authCookie.includes('bt_auth=true')) {
    return next();
  }
  
  // Also allow Authorization header for programmatic access if needed
  if (req.headers.authorization === `Bearer ${AUTH_PASSWORD}`) {
    return next();
  }

  res.status(401).json({ success: false, error: 'Unauthorized' });
};

// Health check endpoint (Public)
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Login endpoint
app.post('/api/login', (req, res) => {
  const { password } = req.body;
  if (password === AUTH_PASSWORD || password === 'fusion-2024') {
    res.json({ success: true });
  } else {
    res.status(401).json({ success: false, error: 'Invalid password' });
  }
});

// Protected routes
app.use('/api', authMiddleware);

// Get the latest known state of all bazaar items
app.get('/api/bazaar', (req, res) => {
  try {
    const lastPrices = getLastRecordedPrices();
    const result: any = {};
    for (const [productId, priceObj] of lastPrices.entries()) {
      result[productId] = {
        product_id: productId,
        buyPrice: priceObj.buy_price,
        sellPrice: priceObj.sell_price,
        buyVolume: priceObj.buy_volume,
        sellVolume: priceObj.sell_volume,
        buyOrders: priceObj.buy_orders,
        sellOrders: priceObj.sell_orders,
        buyMovingWeek: priceObj.buy_moving_week,
        sellMovingWeek: priceObj.sell_moving_week,
        timestamp: priceObj.timestamp
      };
    }
    res.json({ success: true, products: result });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

// Get history for a specific product
app.get('/api/bazaar/history/:productId', (req, res) => {
  try {
    const productId = req.params.productId;
    // Query param to specify resolution: 'raw', '5m', or '1h' (defaults to 'raw')
    const resolution = req.query.resolution as string || (req.query.hourly === 'true' ? '1h' : 'raw');
    const limit = parseInt(req.query.limit as string) || 1000;

    if (resolution === '1d') {
      const history = getDailyHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: '1d', data: history });
    } else if (resolution === '1h') {
      const history = getHourlyHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: '1h', data: history });
    } else if (resolution === '10m') {
      const history = getTenMinHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: '10m', data: history });
    } else if (resolution === '30m') {
      const history = getThirtyMinHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: '30m', data: history });
    } else if (resolution === '5m') {
      const history = getFiveMinHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: '5m', data: history });
    } else if (resolution === '1m') {
      const history = getOneMinHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: '1m', data: history });
    } else {
      const history = getRecentHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: 'raw', data: history });
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

// Unified history endpoint - stitches all resolution tiers into one seamless timeline
// Cached per-product for 30 seconds to avoid heavy queries on every poll
const unifiedCache = new Map<string, { data: any; timestamp: number }>();
const UNIFIED_CACHE_TTL = 30 * 1000; // 30 seconds

app.get('/api/bazaar/history/:productId/unified', (req, res) => {
  try {
    const productId = req.params.productId;
    const now = Date.now();
    const cached = unifiedCache.get(productId);

    if (cached && (now - cached.timestamp < UNIFIED_CACHE_TTL)) {
      return res.json({ success: true, product_id: productId, cached: true, data: cached.data });
    }

    const data = getUnifiedHistory(productId);
    unifiedCache.set(productId, { data, timestamp: now });

    // Evict stale entries periodically (keep cache bounded)
    if (unifiedCache.size > 200) {
      for (const [key, val] of unifiedCache.entries()) {
        if (now - val.timestamp > UNIFIED_CACHE_TTL * 2) unifiedCache.delete(key);
      }
    }

    res.json({ success: true, product_id: productId, cached: false, data });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

// Get live order book (buy/sell summaries) for a specific product
app.get('/api/bazaar/orders/:productId', (req, res) => {
  try {
    const productId = req.params.productId;
    const orders = getLiveOrders(productId);
    
    if (!orders) {
      return res.status(404).json({ success: false, error: 'Live orders not found for this product' });
    }

    res.json({ success: true, product_id: productId, buy_summary: orders.buy_summary, sell_summary: orders.sell_summary });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

// Get volume history for a specific product
app.get('/api/bazaar/volume/:productId', (req, res) => {
  try {
    const productId = req.params.productId;
    const start = parseInt(req.query.start as string) || (Date.now() - 7 * 24 * 60 * 60 * 1000);
    const end = parseInt(req.query.end as string) || Date.now();
    const interval = parseInt(req.query.interval as string) || 3600000; // Default 1 hour

    const history = getVolumeHistory(productId, start, end, interval);
    res.json({ success: true, product_id: productId, data: history });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

// Status & analytics endpoint for the dashboard
app.get('/api/status', (req, res) => {
  try {
    const stats = getStatusStats();
    res.json({ success: true, ...stats, timestamp: Date.now() });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

// Get mayor transitions in a range
app.get('/api/mayors', (req, res) => {
  try {
    const start = parseInt(req.query.start as string) || 0;
    const end = parseInt(req.query.end as string) || Date.now();
    const mayors = getMayorsInRange(start, end);
    res.json({ success: true, data: mayors });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});
// Local memory for ML predictions uploaded by clients
let uploadedPredictions: any = { items: [], total: 0 };

app.post('/api/ml/upload', (req, res) => {
  try {
    uploadedPredictions = req.body;
    console.log(`[ML] Received uploaded predictions. Total items: ${uploadedPredictions.total || 0}`);
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

app.get('/api/ml/predictions', (req, res) => {
  const limit = parseInt(req.query.limit as string) || 100;
  const minScore = parseFloat(req.query.min_score as string) || 0.0;
  
  if (!uploadedPredictions.items) {
     return res.json({ items: [], total: 0 });
  }
  
  const filtered = uploadedPredictions.items
    .filter((p: any) => p.entry_score > minScore)
    .slice(0, limit);
    
  res.json({ items: filtered, total: filtered.length });
});

app.get('/api/ml/predict/:productId', (req, res) => {
  const productId = req.params.productId;
  if (!uploadedPredictions.items) {
     return res.status(404).json({ error: 'No predictions available' });
  }
  const item = uploadedPredictions.items.find((p: any) => p.item_id === productId);
  if (!item) {
     return res.status(404).json({ error: 'Prediction not found for item' });
  }
  res.json(item);
});

import path from 'path';
import fs from 'fs';

app.get('/api/ml/client', (req, res) => {
  const zipPath = path.join(__dirname, '../assets/bazaar-ml-client.zip');
  if (fs.existsSync(zipPath)) {
    res.download(zipPath, 'bazaar-ml-client.zip');
  } else {
    res.status(404).send('Client bundle not found');
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[Server] Bazaar API Backbone running on port ${PORT}`);
  
  // Log heartbeat for status page every minute
  setInterval(() => logHeartbeat('api'), 60000);
  logHeartbeat('api');
});

