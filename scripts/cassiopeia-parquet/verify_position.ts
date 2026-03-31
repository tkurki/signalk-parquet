#!/usr/bin/env node
// Verify the Hive-partitioned navigation.position Parquet output produced by
// convert_position.ts against the original source file.
//
// Run with:
//   node --experimental-strip-types verify_position.ts \
//     --source  ../../stash-cassiopeia-data/cassiopeia_trackpoint.parquet \
//     --data-dir /path/to/signalk-data-dir
//
// --source is optional; when omitted, source-vs-output row count comparison
// is skipped.

import { DuckDBInstance } from '@duckdb/node-api';
import { resolve } from 'path';

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(): { source: string | null; dataDir: string } {
  const args = process.argv.slice(2);
  let source: string | null = null;
  let dataDir = '';
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--source' && args[i + 1]) source = resolve(args[++i]);
    else if (args[i] === '--data-dir' && args[i + 1])
      dataDir = resolve(args[++i]);
  }
  if (!dataDir) {
    console.error(
      'Usage: node --experimental-strip-types verify_position.ts --data-dir <dir> [--source <file>]'
    );
    process.exit(1);
  }
  return { source, dataDir };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function pass(msg: string): void {
  console.log(`  ✓  ${msg}`);
}
function fail(msg: string): void {
  console.error(`  ✗  ${msg}`);
}
function section(title: string): void {
  console.log(`\n── ${title} ──`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const { source, dataDir } = parseArgs();

  const glob =
    `${dataDir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970` +
    `/path=navigation__position/year=*/day=*/*.parquet`;

  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();

  let failures = 0;

  // ------------------------------------------------------------------
  // 1. Row count
  // ------------------------------------------------------------------
  section('Row count');

  const outCountResult = await con.runAndReadAll(
    `SELECT COUNT(*) AS n
     FROM read_parquet('${glob}', hive_partitioning=true)`
  );
  const outRows = (outCountResult.getRowObjects()[0] as { n: bigint }).n;
  console.log(`  Output rows: ${outRows.toLocaleString()}`);

  if (source) {
    const safeSource = source.replace(/'/g, "''");
    const srcResult = await con.runAndReadAll(
      `SELECT COUNT(*) AS n FROM read_parquet('${safeSource}')
       WHERE lat BETWEEN -90 AND 90 AND lng BETWEEN -180 AND 180`
    );
    const srcRows = (srcResult.getRowObjects()[0] as { n: bigint }).n;
    console.log(`  Source valid rows: ${srcRows.toLocaleString()}`);

    if (outRows === srcRows) {
      pass(`Row counts match: ${outRows.toLocaleString()}`);
    } else {
      fail(`Row count mismatch: output=${outRows}, source valid=${srcRows}`);
      failures++;
    }
  } else {
    console.log('  (--source not provided — skipping source comparison)');
  }

  // ------------------------------------------------------------------
  // 2. Schema validation
  // ------------------------------------------------------------------
  section('Schema');

  const schemaResult = await con.runAndReadAll(
    `DESCRIBE SELECT * FROM read_parquet('${glob}', hive_partitioning=true) LIMIT 0`
  );
  const cols = (
    schemaResult.getRowObjects() as Array<{
      column_name: string;
      column_type: string;
    }>
  ).map((r) => ({ name: r.column_name, type: r.column_type }));

  const expected: Array<{ name: string; typePattern: RegExp }> = [
    { name: 'received_timestamp', typePattern: /VARCHAR/i },
    { name: 'signalk_timestamp', typePattern: /VARCHAR/i },
    { name: 'context', typePattern: /VARCHAR/i },
    { name: 'path', typePattern: /VARCHAR/i },
    { name: 'value_latitude', typePattern: /DOUBLE|FLOAT/i },
    { name: 'value_longitude', typePattern: /DOUBLE|FLOAT/i },
    { name: 'source', typePattern: /VARCHAR/i },
    { name: 'source_label', typePattern: /VARCHAR/i },
  ];

  for (const exp of expected) {
    const col = cols.find((c) => c.name === exp.name);
    if (!col) {
      fail(`Missing column: ${exp.name}`);
      failures++;
    } else if (!exp.typePattern.test(col.type)) {
      fail(`Column ${exp.name} has type ${col.type}, expected ${exp.typePattern}`);
      failures++;
    } else {
      pass(`${exp.name}  (${col.type})`);
    }
  }

  // Ensure no unexpected value_* columns (e.g. value column should be absent)
  const unexpectedValueCols = cols.filter(
    (c) =>
      c.name.startsWith('value_') &&
      c.name !== 'value_latitude' &&
      c.name !== 'value_longitude'
  );
  if (unexpectedValueCols.length > 0) {
    fail(
      `Unexpected value_* columns: ${unexpectedValueCols.map((c) => c.name).join(', ')}`
    );
    failures++;
  }
  const valueCol = cols.find((c) => c.name === 'value');
  if (valueCol) {
    fail(
      'Unexpected "value" column present (position paths use exploded value_latitude/value_longitude)'
    );
    failures++;
  }

  // ------------------------------------------------------------------
  // 3. Lat/lng ranges
  // ------------------------------------------------------------------
  section('Lat/lng ranges');

  const rangeResult = await con.runAndReadAll(
    `SELECT
       MIN(value_latitude)  AS min_lat,
       MAX(value_latitude)  AS max_lat,
       MIN(value_longitude) AS min_lng,
       MAX(value_longitude) AS max_lng,
       COUNT_IF(value_latitude  < -90  OR value_latitude  > 90)  AS bad_lat,
       COUNT_IF(value_longitude < -180 OR value_longitude > 180) AS bad_lng
     FROM read_parquet('${glob}', hive_partitioning=true)`
  );
  const range = rangeResult.getRowObjects()[0] as {
    min_lat: number;
    max_lat: number;
    min_lng: number;
    max_lng: number;
    bad_lat: bigint;
    bad_lng: bigint;
  };

  console.log(
    `  Latitude  : ${range.min_lat?.toFixed(6)} → ${range.max_lat?.toFixed(6)}`
  );
  console.log(
    `  Longitude : ${range.min_lng?.toFixed(6)} → ${range.max_lng?.toFixed(6)}`
  );

  if (range.bad_lat === 0n) {
    pass('All value_latitude within [-90, 90]');
  } else {
    fail(`${range.bad_lat} rows have value_latitude outside [-90, 90]`);
    failures++;
  }
  if (range.bad_lng === 0n) {
    pass('All value_longitude within [-180, 180]');
  } else {
    fail(`${range.bad_lng} rows have value_longitude outside [-180, 180]`);
    failures++;
  }

  // ------------------------------------------------------------------
  // 4. Timestamp format spot-check
  // ------------------------------------------------------------------
  section('Timestamp format');

  const tsResult = await con.runAndReadAll(
    `SELECT
       COUNT_IF(NOT regexp_matches(
         signalk_timestamp,
         '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$'
       )) AS bad_ts,
       MIN(signalk_timestamp) AS first_ts,
       MAX(signalk_timestamp) AS last_ts
     FROM read_parquet('${glob}', hive_partitioning=true)`
  );
  const tsRow = tsResult.getRowObjects()[0] as {
    bad_ts: bigint;
    first_ts: string;
    last_ts: string;
  };
  console.log(`  First timestamp: ${tsRow.first_ts}`);
  console.log(`  Last timestamp:  ${tsRow.last_ts}`);
  if (tsRow.bad_ts === 0n) {
    pass('All signalk_timestamp match ISO 8601 with milliseconds');
  } else {
    fail(`${tsRow.bad_ts} rows have malformed signalk_timestamp`);
    failures++;
  }

  // ------------------------------------------------------------------
  // 5. Context and path constants
  // ------------------------------------------------------------------
  section('Context and path constants');

  const ctxResult = await con.runAndReadAll(
    `SELECT DISTINCT context, path
     FROM read_parquet('${glob}', hive_partitioning=true)`
  );
  const ctxRows = ctxResult.getRowObjects() as Array<{
    context: string;
    path: string;
  }>;

  if (ctxRows.length === 1) {
    pass(`Single (context, path) combination`);
  } else {
    fail(`Expected 1 (context, path) combination, found ${ctxRows.length}`);
    failures++;
  }
  for (const r of ctxRows) {
    if (r.context === 'vessels.urn:mrn:imo:mmsi:230029970') {
      pass(`context = ${r.context}`);
    } else {
      fail(`Unexpected context: ${r.context}`);
      failures++;
    }
    if (r.path === 'navigation.position') {
      pass(`path = ${r.path}`);
    } else {
      fail(`Unexpected path: ${r.path}`);
      failures++;
    }
  }

  // ------------------------------------------------------------------
  // 6. Partition count
  // ------------------------------------------------------------------
  section('Partition count');

  const partResult = await con.runAndReadAll(
    `SELECT COUNT(DISTINCT (year, day)) AS n
     FROM read_parquet('${glob}', hive_partitioning=true)`
  );
  const partCount = (partResult.getRowObjects()[0] as { n: bigint }).n;
  console.log(`  Partitions (year, day): ${partCount}`);
  if (partCount >= 90n && partCount <= 100n) {
    pass(`Partition count ${partCount} is within expected range (~96 days)`);
  } else {
    fail(
      `Partition count ${partCount} outside expected range [90, 100] — investigate`
    );
    failures++;
  }

  // ------------------------------------------------------------------
  // 7. Spot-check: first 5 rows
  // ------------------------------------------------------------------
  section('Spot-check (first 5 rows)');

  const spotResult = await con.runAndReadAll(
    `SELECT signalk_timestamp, value_latitude, value_longitude, source
     FROM read_parquet('${glob}', hive_partitioning=true)
     ORDER BY signalk_timestamp
     LIMIT 5`
  );
  const spotRows = spotResult.getRowObjects() as Array<{
    signalk_timestamp: string;
    value_latitude: number;
    value_longitude: number;
    source: string;
  }>;
  for (const r of spotRows) {
    console.log(
      `  ${r.signalk_timestamp}  ` +
        `lat=${r.value_latitude.toFixed(6)}  ` +
        `lng=${r.value_longitude.toFixed(6)}  ` +
        `src=${r.source}`
    );
  }

  // ------------------------------------------------------------------
  // Summary
  // ------------------------------------------------------------------
  console.log();
  if (failures === 0) {
    console.log('All checks passed ✓');
  } else {
    console.error(`${failures} check(s) FAILED ✗`);
    process.exit(1);
  }
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
