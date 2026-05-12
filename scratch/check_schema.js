const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.resolve(__dirname, '..', 'bazaar.db');
console.log('Using DB at:', dbPath);
const db = new Database(dbPath);

try {
  console.log('Ensuring table exists...');
  db.prepare(`
    CREATE TABLE IF NOT EXISTS service_heartbeats (
      service_name TEXT PRIMARY KEY,
      timestamp INTEGER
    )
  `).run();

  const columns = db.prepare("PRAGMA table_info(service_heartbeats)").all();
  console.log('Columns in service_heartbeats:', columns);
  
  if (!columns.find(c => c.name === 'metadata')) {
    console.log('Adding metadata column...');
    db.prepare("ALTER TABLE service_heartbeats ADD COLUMN metadata TEXT").run();
    console.log('Success!');
  } else {
    console.log('Metadata column already exists.');
  }
} catch (err) {
  console.error('Error:', err);
} finally {
  db.close();
}
