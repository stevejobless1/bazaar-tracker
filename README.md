# Hypixel Bazaar Historical Tracker

This service polls the Hypixel SkyBlock Bazaar API every 20 seconds, intelligently tracks changes, and provides a backend API to retrieve the latest and historical prices.

## Data Condensation

This service uses an aggressive **Delta Storage** strategy. It only records a new database row when a product's price or volume *actually changes*. 
It also includes an **Automated Downsampler** that converts data older than 7 days into hourly OHLC (Open, High, Low, Close) candles. This keeps the database incredibly lightweight (~10-30MB/day for recent data, <1MB/day for historical).

## Installation

```sh
npm install
```

## Running the Services

You need to run the Tracker, the Downsampler (optional but recommended), and the API Server. 

### 1. The Tracker (Data Ingestion)
This pulls data from the Bazaar every 20 seconds and saves it to SQLite.
```sh
npx ts-node src/tracker.ts
```

### 2. The API Server
This serves the REST API.
```sh
npx ts-node src/server.ts
```

### 3. The Downsampler
This runs once a day (if kept alive) or whenever executed to compress old data.
```sh
npx ts-node src/downsampler.ts
```

## Deployment (Coolify / Docker)

This project is "Coolify-ready." 

1. **In Coolify:** Select "Docker Compose" as the deployment type.
2. **Point to this repo:** It will automatically detect the `docker-compose.yml`.
3. **Storage:** Coolify will automatically create a persistent volume for the SQLite database. This ensures your historical data is never lost when the container updates.
4. **Environment Variables:**
   - `PORT`: 3000
   - `DB_PATH`: `/app/data/bazaar.db` (This maps to the persistent volume)

## API Endpoints

### Get Latest State
Returns the latest known prices for all items (similar to the official API).
`GET /api/bazaar`

### Get Historical Data (High Resolution)
Returns the most recent exact state changes for a product.
`GET /api/bazaar/history/ENCHANTED_BROWN_MUSHROOM`
*Optional limit:* `?limit=1000`

### Get Historical Data (Hourly Candles)
Returns the highly condensed hourly OHLC candles for a product (data older than 7 days).
`GET /api/bazaar/history/ENCHANTED_BROWN_MUSHROOM?hourly=true`
