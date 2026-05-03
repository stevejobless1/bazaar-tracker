import { insertMayor, initDB } from './db';
import { legacyMayors } from './scripts/mayorData';

export async function seedLegacyMayors() {
  try {
    console.log(`[Seed] Checking for legacy mayor data...`);
    
    // We use INSERT OR IGNORE, so we can just loop through all of them.
    // It's very fast for 1200 records.
    let count = 0;
    for (let i = 0; i < legacyMayors.length; i++) {
      const mayor = legacyMayors[i];
      const startTs = new Date(mayor.start).getTime();
      
      if (isNaN(startTs)) continue;

      let endTs: number;
      if (i < legacyMayors.length - 1) {
        endTs = new Date(legacyMayors[i+1].start).getTime();
      } else {
        endTs = startTs + 450000000; // ~5.2 days
      }

      insertMayor(mayor.name, startTs, endTs);
      count++;
    }
    
    if (count > 0) {
      console.log(`[Seed] Legacy mayor seed check complete. (Ensured ${count} records)`);
    }
  } catch (err) {
    console.error(`[Seed] Failed to seed legacy mayors:`, err);
  }
}
