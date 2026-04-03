#!/usr/bin/env node
// Convert cassiopeia_value.parquet → Hive-partitioned scalar-value Parquet files.
//
// Strategy (two phases):
//   Phase 1 — single pass through the source file, fanning out to one CSV
//              staging file per SignalK path (DuckDB COPY TO PARTITION_BY).
//   Phase 2 — for each per-path CSV, write one Parquet file per day into
//              the Hive partition tree.
//
// Run with:
//   node --experimental-strip-types convert_values.ts \
//     --input  ../../stash-cassiopeia-data/cassiopeia_value.parquet \
//     --output /path/to/signalk-data-dir
//   [--keep-csv]    keep the _csv_staging directory after conversion
//   [--from-csv]    skip Phase 1, run Phase 2 from existing _csv_staging
//                   (--input is not required when using --from-csv)

import { DuckDBInstance } from '@duckdb/node-api';
import { mkdirSync, readdirSync, rmSync } from 'fs';
import { join, resolve } from 'path';

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(): { input: string; output: string; keepCsv: boolean; fromCsv: boolean } {
  const args = process.argv.slice(2);
  let input = '';
  let output = '';
  let keepCsv = false;
  let fromCsv = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--input' && args[i + 1]) input = args[++i];
    else if (args[i] === '--output' && args[i + 1]) output = args[++i];
    else if (args[i] === '--keep-csv') keepCsv = true;
    else if (args[i] === '--from-csv') fromCsv = true;
  }
  if (!output || (!input && !fromCsv)) {
    console.error(
      'Usage: node --experimental-strip-types convert_values.ts --input <file> --output <dir> [--keep-csv] [--from-csv]'
    );
    process.exit(1);
  }
  return { input: input ? resolve(input) : '', output: resolve(output), keepCsv, fromCsv };
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

/** Reverse of encodeHivePath (__ → .) — valid because SignalK paths only use dots. */
function decodeHivePath(pathEnc: string): string {
  return pathEnc.replace(/__/g, '.');
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
  const { input, output, keepCsv, fromCsv } = parseArgs();

  const csvDir = join(output, '_csv_staging');

  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();

  console.log(`Output: ${output}`);

  // ==================================================================
  // Phase 1: Single pass — fan out to per-path CSV files
  // ==================================================================
  if (fromCsv) {
    console.log('--from-csv: skipping Phase 1, using existing CSV staging dir.');
    console.log(`CSV dir: ${csvDir}`);
    console.log();
  } else {
    const safeInput = input.replace(/'/g, "''");
    const safeCsvDir = csvDir.replace(/'/g, "''");

    const totalResult = await con.runAndReadAll(
      `SELECT COUNT(*) AS n FROM read_parquet('${safeInput}')`
    );
    const totalRows = (totalResult.getRowObjects()[0] as { n: bigint }).n;
    console.log(`Input:  ${input}`);
    console.log(`Source rows: ${totalRows.toLocaleString()}`);
    console.log();

    mkdirSync(csvDir, { recursive: true });

    console.log('Phase 1: streaming source → per-path CSV files…');
    const t1 = Date.now();

    // DuckDB COPY TO with PARTITION_BY does a single scan and writes one or
    // more CSV chunk files per partition directory, e.g.:
    //   _csv_staging/path_enc=navigation__speedOverGround/data_0.csv
    //
    // The partition column (path_enc) is stripped from the CSV content, so
    // we retain the decoded `path` column separately.
    // `day_epoch` is included so Phase 2 can partition by day cheaply.
    await con.runAndReadAll(`
      COPY (
        SELECT
          strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
              || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z'
              AS received_timestamp,
          strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
              || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z'
              AS signalk_timestamp,
          'vessels.' || CAST(context  AS VARCHAR) AS context,
          CAST(path AS VARCHAR)                   AS path,
          replace(CAST(path AS VARCHAR), '.', '__') AS path_enc,
          CAST(ts / 86400 AS INTEGER)             AS day_epoch,
          CAST(value AS DOUBLE)                   AS value,
          CAST(sourceRef AS VARCHAR)              AS source,
          CAST(sourceRef AS VARCHAR)              AS source_label
        FROM read_parquet('${safeInput}')
      ) TO '${safeCsvDir}' (FORMAT CSV, PARTITION_BY (path_enc), OVERWRITE_OR_IGNORE)
    `);

    const elapsed1 = ((Date.now() - t1) / 1000).toFixed(1);
    console.log(`Phase 1 complete in ${elapsed1}s.`);
    console.log();
  }

  // ==================================================================
  // Phase 2: Per-path CSVs → per-day Hive-partitioned Parquet files
  // ==================================================================
  console.log('Phase 2: per-path CSV → per-day Parquet…');
  const t2 = Date.now();

  // Each subdirectory is named "path_enc=<encodedPath>"
  const pathDirs = readdirSync(csvDir).filter((d) =>
    d.startsWith('path_enc=')
  );
  console.log(`Paths found: ${pathDirs.length}`);
  console.log();

  let totalParquet = 0;

  for (let pi = 0; pi < pathDirs.length; pi++) {
    const pathEnc = pathDirs[pi].slice('path_enc='.length);
    const pathStr = decodeHivePath(pathEnc);
    const safeCsvGlob = join(csvDir, pathDirs[pi], '*.csv').replace(/'/g, "''");

    // Find all distinct days present in this path's CSV(s)
    const dayResult = await con.runAndReadAll(
      `SELECT DISTINCT day_epoch FROM read_csv('${safeCsvGlob}') ORDER BY 1`
    );
    const days = (
      dayResult.getRowObjects() as Array<{ day_epoch: number | bigint }>
    ).map((r) => Number(r.day_epoch));

    for (const dayEpoch of days) {
      const d = dayEpochToDate(dayEpoch);
      const { dir, file } = buildHivePaths(output, pathStr, d);
      const safeFile = file.replace(/'/g, "''");

      mkdirSync(dir, { recursive: true });

      await con.runAndReadAll(`
        COPY (
          SELECT received_timestamp, signalk_timestamp, context, path,
                 value, source, source_label
          FROM read_csv('${safeCsvGlob}')
          WHERE day_epoch = ${dayEpoch}
          ORDER BY received_timestamp
        ) TO '${safeFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
      `);

      totalParquet++;
    }

    const pct = (((pi + 1) / pathDirs.length) * 100).toFixed(1);
    console.log(
      `  [${pad(pi + 1, 3)}/${pathDirs.length}]  ${pct}%` +
        `  ${days.length} days  ${pathStr}`
    );
  }

  const elapsed2 = ((Date.now() - t2) / 1000).toFixed(1);
  console.log();
  console.log(
    `Phase 2 complete in ${elapsed2}s. Parquet files written: ${totalParquet.toLocaleString()}`
  );

  // ==================================================================
  // Cleanup
  // ==================================================================
  if (!keepCsv) {
    console.log('Removing CSV staging directory…');
    rmSync(csvDir, { recursive: true, force: true });
  } else {
    console.log(`CSV staging kept at: ${csvDir}`);
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
