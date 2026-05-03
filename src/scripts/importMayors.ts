import { insertMayor, initDB } from '../db';
import { legacyMayors } from './mayorData';

async function importMayors() {
  console.log(`[Import] Starting legacy mayor import (Embedded Data)...`);

  let count = 0;
  let skipped = 0;

  // Ensure DB is initialized
  initDB();

  for (const mayor of legacyMayors) {
    try {
      // Date.parse handles "MM/DD/YYYY HH:MM:SS +00:00"
      const timestamp = new Date(mayor.start).getTime();
      
      if (isNaN(timestamp)) {
        console.warn(`[Import] Warning: Invalid date format for mayor: ${mayor.name} (${mayor.start})`);
        skipped++;
        continue;
      }

      insertMayor(timestamp, mayor.name);
      count++;
    } catch (err) {
      console.error(`[Import] Failed to import mayor: ${mayor.name}`, err);
      skipped++;
    }
  }

  console.log(`[Import] Completed! Successfully imported ${count} mayors. Skipped ${skipped}.`);
}

importMayors().catch(err => {
  console.error('[Import] Critical error during import:', err);
  process.exit(1);
});
