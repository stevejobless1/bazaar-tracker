import Database from 'better-sqlite3';
import path from 'path';

// Define the database path (matching db.ts logic)
const dbPath = process.env.DB_PATH 
  ? path.resolve(process.env.DB_PATH) 
  : path.resolve(__dirname, '../bazaar.db');

const service = process.argv[2];

if (!service) {
  console.error('Usage: node healthcheck.js <service_name>');
  process.exit(1);
}

try {
  const db = new Database(dbPath, { readonly: true });
  
  // Check if the service has logged a heartbeat in the last 2 minutes
  const row = db.prepare(`
    SELECT timestamp 
    FROM service_heartbeats 
    WHERE service_name = ? 
    ORDER BY timestamp DESC 
    LIMIT 1
  `).get(service) as { timestamp: number } | undefined;

  db.close();

  if (row) {
    const now = Date.now();
    const ageMs = now - row.timestamp;
    const twoMinutesMs = 2 * 60 * 1000;

    if (ageMs < twoMinutesMs) {
      console.log(`[Healthcheck] Service ${service} is healthy (Last heartbeat: ${Math.round(ageMs/1000)}s ago)`);
      process.exit(0);
    } else {
      console.error(`[Healthcheck] Service ${service} is UNHEALTHY! Last heartbeat was ${Math.round(ageMs/1000)}s ago (Limit: 120s)`);
      process.exit(1);
    }
  } else {
    console.error(`[Healthcheck] Service ${service} is UNHEALTHY! No heartbeat found in service_heartbeats table.`);
    process.exit(1);
  }
} catch (err) {
  console.error(`[Healthcheck] Error checking health for ${service}:`, err);
  process.exit(1);
}
