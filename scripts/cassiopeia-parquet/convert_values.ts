#!/usr/bin/env node
// Convert cassiopeia_value.parquet → Hive-partitioned scalar-value Parquet files.
//
// Run with:
//   node --experimental-strip-types convert_values.ts \
//     --input  ../../stash-cassiopeia-data/cassiopeia_value.parquet \
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
      'Usage: node --experimental-strip-types convert_values.ts --input <file> --output <dir>'
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

/** Encode a SignalK path for use in a Hive directory name: dots → __ */
function encodeHivePath(path: string): string {
  return path.replace(/\./g, '__');
}

function buildHivePaths(
  outputDir: string,
  pathStr: string,
  d: Date
): { dir: string; file: string } {
  const year = d.getUTCFullYear();
  const doy = utcDayOfYear(d);
  const mm = pad(d.getUTCMonth() + 1, 2);
  const dd = pad(d.getUTCDate(), 2);
  const pathEnc = encodeHivePath(pathStr);

  const dir = join(
    outputDir,
    'tier=raw',
    'context=vessels__urn-mrn-imo-mmsi-230029970',
    `path=${pathEnc}`,
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

  console.log(`Input:  ${input}`);
  console.log(`Output: ${output}`);
  console.log(`Source rows: ${totalRows.toLocaleString()}`);
  console.log();

  // ------------------------------------------------------------------
  // Step 1: Enumerate distinct (path, day_epoch) partitions
  // ------------------------------------------------------------------
  console.log('Enumerating partitions (path × day)…');
  const safeInput = input.replace(/'/g, "''");
  const partResult = await con.runAndReadAll(
    `SELECT CAST(path AS VARCHAR) AS p,
            CAST(ts / 86400 AS INTEGER) AS day_epoch
     FROM read_parquet('${safeInput}')
     GROUP BY 1, 2
     ORDER BY 1, 2`
  );
  const partitions = partResult.getRowObjects() as Array<{
    p: string;
    day_epoch: number;
  }>;

  console.log(`Partitions (path × day): ${partitions.length.toLocaleString()}`);
  console.log();

  // ------------------------------------------------------------------
  // Steps 2–4: Transform and write each partition via COPY TO
  // ------------------------------------------------------------------
  for (let i = 0; i < partitions.length; i++) {
    const { p: pathStr, day_epoch: dayEpoch } = partitions[i];
    const d = dayEpochToDate(dayEpoch);
    const doy = utcDayOfYear(d);
    const { dir, file } = buildHivePaths(output, pathStr, d);

    mkdirSync(dir, { recursive: true });

    // Escape single-quotes in path (e.g. unlikely but safe)
    const safePathStr = pathStr.replace(/'/g, "''");
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
          CAST(path     AS VARCHAR)               AS path,
          CAST(value    AS DOUBLE)                AS value,
          CAST(sourceRef AS VARCHAR)              AS source,
          CAST(sourceRef AS VARCHAR)              AS source_label
        FROM read_parquet('${safeInput}')
        WHERE CAST(path    AS VARCHAR) = '${safePathStr}'
          AND CAST(ts / 86400 AS INTEGER) = ${dayEpoch}
        ORDER BY ts, millis
      ) TO '${safeFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
    `;

    await con.runAndReadAll(copySQL);

    if ((i + 1) % 100 === 0 || i + 1 === partitions.length) {
      const pct = (((i + 1) / partitions.length) * 100).toFixed(1);
      console.log(
        `  [${pad(i + 1, 5)}/${partitions.length}]  ${pct}%` +
          `  ${d.toISOString().slice(0, 10)}  day=${pad(doy, 3)}  ${pathStr}`
      );
    }
  }

  console.log();
  console.log('Raw conversion complete.');
  console.log(
    `Next step: node --experimental-strip-types aggregate_values.ts --data-dir ${output}`
  );
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
