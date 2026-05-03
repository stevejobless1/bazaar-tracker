import express from 'express';
import cors from 'cors';
import compression from 'compression';
import { 
  getLastRecordedPrices, 
  getRecentHistory, 
  getOneMinHistory,
  getFiveMinHistory, 
  getThirtyMinHistory,
  getHourlyHistory, 
  getLiveOrders, 
  getStatusStats,
  getMayorsInRange,
  logHeartbeat
} from './db';

const app = express();
app.use(cors());
app.use(compression());

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

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

    if (resolution === '1h') {
      const history = getHourlyHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: '1h', data: history });
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[Server] Bazaar API Backbone running on port ${PORT}`);
  
  // Log heartbeat for status page every minute
  setInterval(() => logHeartbeat('api'), 60000);
  logHeartbeat('api');
});

