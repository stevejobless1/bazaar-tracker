import { initDB } from './db';

export async function seedLegacyMayors() {
  // Legacy mayor seeding is now handled by manual import scripts (src/scripts/importMayorsFromFile.ts)
  // to ensure full historical data is present in the database.
  console.log(`[Seed] Historical mayor data is managed via the database.`);
}
