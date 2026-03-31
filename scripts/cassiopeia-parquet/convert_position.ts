#!/usr/bin/env node
// Convert cassiopeia_trackpoint.parquet → Hive-partitioned navigation.position Parquet files.
//
// Run with:
//   node --experimental-strip-types convert_position.ts \
//     --input  ../../stash-cassiopeia-data/cassiopeia_trackpoint.parquet \
//     --output /path/to/signalk-data-dir

import { DuckDBInstance } from '@duckdb/node-api';
import { mkdirSync } from 'fs';
import { join, resolve } from 'path';

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(): { input: string; output: string } {
  const args = process.argv.slice(2);
  let input = '';
  let output = '';
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--input' && args[i + 1]) input = args[++i];
    else if (args[i] === '--output' && args[i + 1]) output = args[++i];
  }
  if (!input || !output) {
    console.error(
      'Usage: node --experimental-strip-types convert_position.ts --input <file> --output <dir>'
    );
    process.exit(1);
  }
  return { input: resolve(input), output: resolve(output) };
}

// ---------------------------------------------------------------------------
// Date / Hive-path helpers
// ---------------------------------------------------------------------------

function dayEpochToDate(dayEpoch: number): Date {
  return new Date(dayEpoch * 86400 * 1000);
}

/** Day-of-year (1-based) in UTC. */
function utcDayOfYear(d: Date): number {
  const yearStart = Date.UTC(d.getUTCFullYear(), 0, 1);
  const dayMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  return Math.floor((dayMs - yearStart) / 86_400_000) + 1;
}

function pad(n: number, w: number): string {
  return String(n).padStart(w, '0');
}

function buildHivePaths(
  outputDir: string,
  d: Date
): { dir: string; file: string } {
  const year = d.getUTCFullYear();
  const doy = utcDayOfYear(d);
  const mm = pad(d.getUTCMonth() + 1, 2);
  const dd = pad(d.getUTCDate(), 2);

  const dir = join(
    outputDir,
    'tier=raw',
    'context=vessels__urn-mrn-imo-mmsi-230029970',
    'path=navigation__position',
    `year=${year}`,
    `day=${pad(doy, 3)}`
  );
  const file = join(dir, `data_${year}${mm}${dd}T000000.parquet`);
  return { dir, file };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const { input, output } = parseArgs();

  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();

  // ------------------------------------------------------------------
  // Summary counts
  // ------------------------------------------------------------------
  const totalResult = await con.runAndReadAll(
    `SELECT COUNT(*) AS n FROM read_parquet('${input}')`
  );
  const totalRows = (totalResult.getRowObjects()[0] as { n: bigint }).n;

  const filteredResult = await con.runAndReadAll(
    `SELECT COUNT(*) AS n FROM read_parquet('${input}')
     WHERE lat < -90 OR lat > 90 OR lng < -180 OR lng > 180`
  );
  const filteredRows = (filteredResult.getRowObjects()[0] as { n: bigint }).n;

  console.log(`Input:  ${input}`);
  console.log(`Output: ${output}`);
  console.log();
  console.log(`Source rows:   ${totalRows.toLocaleString()}`);
  console.log(
    `Filtered rows: ${filteredRows.toLocaleString()}  (out-of-range lat/lng)`
  );
  console.log(
    `Valid rows:    ${(totalRows - filteredRows).toLocaleString()}`
  );
  console.log();

  // ------------------------------------------------------------------
  // Step 1: enumerate day partitions
  // ------------------------------------------------------------------
  const partResult = await con.runAndReadAll(
    `SELECT CAST(ts / 86400 AS INTEGER) AS day_epoch
     FROM read_parquet('${input}')
     WHERE lat BETWEEN -90 AND 90
       AND lng BETWEEN -180 AND 180
     GROUP BY 1
     ORDER BY 1`
  );
  const dayEpochs = (
    partResult.getRowObjects() as Array<{ day_epoch: number }>
  ).map((r) => r.day_epoch);

  console.log(`Day partitions: ${dayEpochs.length}`);
  console.log();

  // ------------------------------------------------------------------
  // Steps 2 + 3: transform and write one partition at a time
  // ------------------------------------------------------------------
  for (let i = 0; i < dayEpochs.length; i++) {
    const dayEpoch = dayEpochs[i];
    const d = dayEpochToDate(dayEpoch);
    const doy = utcDayOfYear(d);
    const { dir, file } = buildHivePaths(output, d);

    mkdirSync(dir, { recursive: true });

    // Escape any single quotes in the file path (unlikely but safe)
    const safeInput = input.replace(/'/g, "''");
    const safeFile = file.replace(/'/g, "''");

    const copySQL = `
      COPY (
        SELECT
          strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
              || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z'
              AS received_timestamp,
          strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
              || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z'
              AS signalk_timestamp,
          'vessels.' || CAST(context  AS VARCHAR) AS context,
          'navigation.position'                   AS path,
          lat                                     AS value_latitude,
          lng                                     AS value_longitude,
          CAST(sourceRef AS VARCHAR)              AS source,
          CAST(sourceRef AS VARCHAR)              AS source_label
        FROM read_parquet('${safeInput}')
        WHERE lat BETWEEN -90 AND 90
          AND lng BETWEEN -180 AND 180
          AND CAST(ts / 86400 AS INTEGER) = ${dayEpoch}
        ORDER BY ts, millis
      ) TO '${safeFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
    `;

    await con.runAndReadAll(copySQL);

    // Row count for progress display — read back cheaply
    const countResult = await con.runAndReadAll(
      `SELECT COUNT(*) AS n FROM read_parquet('${safeFile}')`
    );
    const rowCount = (countResult.getRowObjects()[0] as { n: bigint }).n;

    console.log(
      `  [${pad(i + 1, 3)}/${dayEpochs.length}]` +
        `  ${d.toISOString().slice(0, 10)}` +
        `  day=${pad(doy, 3)}` +
        `  ${rowCount.toLocaleString().padStart(7)} rows` +
        `  → ${file}`
    );
  }

  console.log();
  console.log('Done.');
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
