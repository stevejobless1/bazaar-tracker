import fs from 'fs';
import path from 'path';
import { insertMayor, initDB } from '../db';

async function importMayorsFromFile() {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error("Please provide the path to the mayors.txt file.");
    process.exit(1);
  }

  const resolvedPath = path.resolve(filePath);
  if (!fs.existsSync(resolvedPath)) {
    console.error(`File not found: ${resolvedPath}`);
    process.exit(1);
  }

  console.log(`[Import] Parsing mayors from ${resolvedPath}...`);
  initDB();

  const content = fs.readFileSync(resolvedPath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim().startsWith('Mayor:'));

  let count = 0;
  let skipped = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    try {
      // Format: Mayor: Foxy | Start: 10/07/2021 07:15:00 +00:00 | End: 10/12/2021 11:15:00
      const nameMatch = line.match(/Mayor:\s*([^|]+)/);
      const startMatch = line.match(/Start:\s*([^|]+)/);
      const endMatch = line.match(/End:\s*(.+)$/);

      if (!nameMatch || !startMatch) {
        console.warn(`[Import] Could not parse line: ${line}`);
        skipped++;
        continue;
      }

      const name = nameMatch[1].trim();
      const startStr = startMatch[1].trim();
      const startTs = new Date(startStr).getTime();

      if (isNaN(startTs)) {
        console.warn(`[Import] Invalid start date for ${name}: ${startStr}`);
        skipped++;
        continue;
      }

      let endTs: number;
      if (endMatch) {
        const endStr = endMatch[1].trim();
        endTs = new Date(endStr).getTime();
        if (isNaN(endTs)) {
          // Fallback to next mayor's start if provided end date is invalid
          if (i < lines.length - 1) {
            const nextStartMatch = lines[i+1].match(/Start:\s*([^|]+)/);
            if (nextStartMatch) {
              endTs = new Date(nextStartMatch[1].trim()).getTime();
            } else {
              endTs = startTs + 450000000;
            }
          } else {
            endTs = startTs + 450000000;
          }
        }
      } else if (i < lines.length - 1) {
        const nextStartMatch = lines[i+1].match(/Start:\s*([^|]+)/);
        if (nextStartMatch) {
          endTs = new Date(nextStartMatch[1].trim()).getTime();
        } else {
          endTs = startTs + 450000000;
        }
      } else {
        endTs = startTs + 450000000;
      }

      insertMayor(name, startTs, endTs);
      count++;
    } catch (err) {
      console.error(`[Import] Error processing line: ${line}`, err);
      skipped++;
    }
  }

  console.log(`[Import] Completed! Successfully imported ${count} mayors. Skipped ${skipped}.`);
}

importMayorsFromFile().catch(err => {
  console.error('[Import] Critical error:', err);
  process.exit(1);
});
