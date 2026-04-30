import express from 'express';
import cors from 'cors';
import { getLastRecordedPrices, getRecentHistory, getHourlyHistory } from './db';

const app = express();
app.use(cors());

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
    // Query param to specify if we want hourly candles or high-res raw data
    const useHourly = req.query.hourly === 'true';
    const limit = parseInt(req.query.limit as string) || 1000;

    if (useHourly) {
      const history = getHourlyHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: 'hourly', data: history });
    } else {
      const history = getRecentHistory(productId, limit);
      res.json({ success: true, product_id: productId, resolution: 'high', data: history });
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[Server] Bazaar API Backbone running on port ${PORT}`);
});
