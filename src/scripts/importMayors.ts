import { insertMayor, initDB } from '../db';
import { legacyMayors } from './mayorData';

async function importMayors() {
  console.log(`[Import] Starting legacy mayor import (Embedded Data)...`);

  let count = 0;
  let skipped = 0;

  // Ensure DB is initialized
  initDB();

  for (let i = 0; i < legacyMayors.length; i++) {
    const mayor = legacyMayors[i];
    try {
      const startTs = new Date(mayor.start).getTime();

      if (isNaN(startTs)) {
        console.warn(`[Import] Warning: Invalid date format for mayor: ${mayor.name} (${mayor.start})`);
        skipped++;
        continue;
      }

      // Infer end date from next mayor's start, or 5.2 days later if it's the last one
      let endTs: number;
      if (i < legacyMayors.length - 1) {
        endTs = new Date(legacyMayors[i + 1].start).getTime();
      } else {
        endTs = startTs + 450000000; // ~5.2 days
      }

      insertMayor(mayor.name, startTs, endTs);
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
