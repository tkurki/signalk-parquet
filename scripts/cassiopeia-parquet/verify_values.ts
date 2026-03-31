#!/usr/bin/env node
// Verify Hive-partitioned value Parquet output from convert_values.ts and
// aggregate_values.ts.
//
// Run with:
//   node --experimental-strip-types verify_values.ts \
//     --data-dir /path/to/signalk-data-dir \
//     [--source ../../stash-cassiopeia-data/cassiopeia_value.parquet]
//
// --source is optional; when supplied, source vs output row counts are compared.

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
      'Usage: node --experimental-strip-types verify_values.ts --data-dir <dir> [--source <file>]'
    );
    process.exit(1);
  }
  return { source, dataDir };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CONTEXT_ENC = 'vessels__urn-mrn-imo-mmsi-230029970';

function glob(dataDir: string, tier: string): string {
  return (
    `${dataDir}/tier=${tier}/context=${CONTEXT_ENC}` +
    `/path=*/year=*/day=*/*.parquet`
  );
}

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

  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();

  let failures = 0;

  const rawGlob = glob(dataDir, 'raw');
  const fiveSecGlob = glob(dataDir, '5s');
  const sixtySecGlob = glob(dataDir, '60s');
  const oneHourGlob = glob(dataDir, '1h');

  // ------------------------------------------------------------------
  // 1. Row counts
  // ------------------------------------------------------------------
  section('Row counts');

  const rawCountResult = await con.runAndReadAll(
    `SELECT COUNT(*) AS n FROM read_parquet('${rawGlob}', hive_partitioning=true)`
  );
  const rawRows = (rawCountResult.getRowObjects()[0] as { n: bigint }).n;
  console.log(`  Raw rows:  ${rawRows.toLocaleString()}`);

  if (source) {
    const safeSource = source.replace(/'/g, "''");
    const srcResult = await con.runAndReadAll(
      `SELECT COUNT(*) AS n FROM read_parquet('${safeSource}')`
    );
    const srcRows = (srcResult.getRowObjects()[0] as { n: bigint }).n;
    console.log(`  Source rows: ${srcRows.toLocaleString()}`);
    if (rawRows === srcRows) {
      pass(`Raw row count matches source: ${rawRows.toLocaleString()}`);
    } else {
      fail(`Row count mismatch: raw=${rawRows}, source=${srcRows}`);
      failures++;
    }
  } else {
    console.log('  (--source not provided — skipping source comparison)');
  }

  // Aggregated tier row counts
  type TierRow = { n: bigint };
  const [r5s, r60s, r1h] = await Promise.all([
    con.runAndReadAll(`SELECT COUNT(*) AS n FROM read_parquet('${fiveSecGlob}', hive_partitioning=true)`),
    con.runAndReadAll(`SELECT COUNT(*) AS n FROM read_parquet('${sixtySecGlob}', hive_partitioning=true)`),
    con.runAndReadAll(`SELECT COUNT(*) AS n FROM read_parquet('${oneHourGlob}', hive_partitioning=true)`),
  ]);
  const rows5s  = (r5s.getRowObjects()[0]  as TierRow).n;
  const rows60s = (r60s.getRowObjects()[0] as TierRow).n;
  const rows1h  = (r1h.getRowObjects()[0]  as TierRow).n;

  console.log(`  5s rows:   ${rows5s.toLocaleString()}`);
  console.log(`  60s rows:  ${rows60s.toLocaleString()}`);
  console.log(`  1h rows:   ${rows1h.toLocaleString()}`);

  if (rows5s < rawRows) {
    pass('5s has fewer rows than raw (aggregation reduced row count)');
  } else {
    fail(`5s row count (${rows5s}) should be < raw (${rawRows})`);
    failures++;
  }
  if (rows60s < rows5s) {
    pass('60s has fewer rows than 5s');
  } else {
    fail(`60s row count (${rows60s}) should be < 5s (${rows5s})`);
    failures++;
  }
  if (rows1h < rows60s) {
    pass('1h has fewer rows than 60s');
  } else {
    fail(`1h row count (${rows1h}) should be < 60s (${rows60s})`);
    failures++;
  }

  // ------------------------------------------------------------------
  // 2. Raw schema
  // ------------------------------------------------------------------
  section('Raw schema');

  const rawSchemaResult = await con.runAndReadAll(
    `DESCRIBE SELECT * FROM read_parquet('${rawGlob}', hive_partitioning=true) LIMIT 0`
  );
  const rawCols = rawSchemaResult.getRowObjects() as Array<{
    column_name: string;
    column_type: string;
  }>;

  const expectedRaw: Array<{ name: string; typePattern: RegExp }> = [
    { name: 'received_timestamp', typePattern: /VARCHAR/i },
    { name: 'signalk_timestamp',  typePattern: /VARCHAR/i },
    { name: 'context',            typePattern: /VARCHAR/i },
    { name: 'path',               typePattern: /VARCHAR/i },
    { name: 'value',              typePattern: /DOUBLE|FLOAT/i },
    { name: 'source',             typePattern: /VARCHAR/i },
    { name: 'source_label',       typePattern: /VARCHAR/i },
  ];
  for (const exp of expectedRaw) {
    const col = rawCols.find((c) => c.column_name === exp.name);
    if (!col) {
      fail(`Missing raw column: ${exp.name}`);
      failures++;
    } else if (!exp.typePattern.test(col.column_type)) {
      fail(`Raw column ${exp.name}: type=${col.column_type}, expected ${exp.typePattern}`);
      failures++;
    } else {
      pass(`${exp.name}  (${col.column_type})`);
    }
  }

  // Ensure no exploded value_* columns exist in raw (position-style)
  const unexpectedExpanded = rawCols.filter((c) =>
    c.column_name.startsWith('value_')
  );
  if (unexpectedExpanded.length > 0) {
    fail(
      `Unexpected exploded value_* columns in raw: ${unexpectedExpanded.map((c) => c.column_name).join(', ')}`
    );
    failures++;
  }

  // ------------------------------------------------------------------
  // 3. Aggregated schema (5s tier)
  // ------------------------------------------------------------------
  section('Aggregated schema (5s tier)');

  const aggSchemaResult = await con.runAndReadAll(
    `DESCRIBE SELECT * FROM read_parquet('${fiveSecGlob}', hive_partitioning=true) LIMIT 0`
  );
  const aggCols = aggSchemaResult.getRowObjects() as Array<{
    column_name: string;
    column_type: string;
  }>;

  const expectedAgg: Array<{ name: string; typePattern: RegExp }> = [
    { name: 'bucket_time',   typePattern: /TIMESTAMP/i },
    { name: 'context',       typePattern: /VARCHAR/i },
    { name: 'path',          typePattern: /VARCHAR/i },
    { name: 'value_avg',     typePattern: /DOUBLE|FLOAT/i },
    { name: 'sample_count',  typePattern: /BIGINT|INT/i },
    { name: 'first_timestamp', typePattern: /VARCHAR/i },
    { name: 'last_timestamp',  typePattern: /VARCHAR/i },
  ];
  for (const exp of expectedAgg) {
    const col = aggCols.find((c) => c.column_name === exp.name);
    if (!col) {
      fail(`Missing aggregated column: ${exp.name}`);
      failures++;
    } else if (!exp.typePattern.test(col.column_type)) {
      fail(`Aggregated column ${exp.name}: type=${col.column_type}, expected ${exp.typePattern}`);
      failures++;
    } else {
      pass(`${exp.name}  (${col.column_type})`);
    }
  }

  // ------------------------------------------------------------------
  // 4. Value sanity (raw tier)
  // ------------------------------------------------------------------
  section('Value sanity (raw tier)');

  const sanityResult = await con.runAndReadAll(
    `SELECT
       COUNT_IF(value IS NULL)                            AS null_count,
       COUNT_IF(TRY_CAST(value AS DOUBLE) IS NULL AND value IS NOT NULL) AS cast_fail,
       MIN(value)                                         AS min_val,
       MAX(value)                                         AS max_val
     FROM read_parquet('${rawGlob}', hive_partitioning=true)`
  );
  const sanity = sanityResult.getRowObjects()[0] as {
    null_count: bigint;
    cast_fail: bigint;
    min_val: number;
    max_val: number;
  };
  console.log(`  value range: ${sanity.min_val} → ${sanity.max_val}`);
  if (sanity.null_count === 0n) {
    pass('No NULL values in raw.value');
  } else {
    fail(`${sanity.null_count} NULL values in raw.value`);
    failures++;
  }
  if (sanity.cast_fail === 0n) {
    pass('All raw.value castable to DOUBLE');
  } else {
    fail(`${sanity.cast_fail} rows where value cannot be cast to DOUBLE`);
    failures++;
  }

  // ------------------------------------------------------------------
  // 5. Timestamp format (raw)
  // ------------------------------------------------------------------
  section('Timestamp format (raw)');

  const tsResult = await con.runAndReadAll(
    `SELECT
       COUNT_IF(NOT regexp_matches(
         received_timestamp,
         '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$'
       )) AS bad_ts,
       MIN(received_timestamp) AS first_ts,
       MAX(received_timestamp) AS last_ts
     FROM read_parquet('${rawGlob}', hive_partitioning=true)`
  );
  const tsRow = tsResult.getRowObjects()[0] as {
    bad_ts: bigint;
    first_ts: string;
    last_ts: string;
  };
  console.log(`  First: ${tsRow.first_ts}`);
  console.log(`  Last:  ${tsRow.last_ts}`);
  if (tsRow.bad_ts === 0n) {
    pass('All received_timestamp match ISO 8601 with milliseconds');
  } else {
    fail(`${tsRow.bad_ts} rows have malformed received_timestamp`);
    failures++;
  }

  // ------------------------------------------------------------------
  // 6. Distinct paths count
  // ------------------------------------------------------------------
  section('Distinct paths');

  const pathCountResult = await con.runAndReadAll(
    `SELECT COUNT(DISTINCT path) AS n FROM read_parquet('${rawGlob}', hive_partitioning=true)`
  );
  const pathCount = (pathCountResult.getRowObjects()[0] as { n: bigint }).n;
  console.log(`  Distinct paths in raw: ${pathCount}`);
  if (pathCount >= 170n && pathCount <= 180n) {
    pass(`Path count ${pathCount} matches expected ~175`);
  } else {
    fail(`Path count ${pathCount} outside expected range [170, 180]`);
    failures++;
  }

  // ------------------------------------------------------------------
  // 7. Top 10 paths by row count
  // ------------------------------------------------------------------
  section('Top 10 paths by row count (raw)');

  const topPathsResult = await con.runAndReadAll(
    `SELECT path, COUNT(*) AS cnt
     FROM read_parquet('${rawGlob}', hive_partitioning=true)
     GROUP BY path
     ORDER BY cnt DESC
     LIMIT 10`
  );
  const topPaths = topPathsResult.getRowObjects() as Array<{
    path: string;
    cnt: bigint;
  }>;
  for (const r of topPaths) {
    console.log(`  ${r.cnt.toLocaleString().padStart(12)}  ${r.path}`);
  }

  // ------------------------------------------------------------------
  // 8. Spot-check: navigation.speedOverGround
  // ------------------------------------------------------------------
  section('Spot-check: navigation.speedOverGround (raw, first 5 rows)');

  const spotResult = await con.runAndReadAll(
    `SELECT received_timestamp, value, source
     FROM read_parquet('${rawGlob}', hive_partitioning=true)
     WHERE path = 'navigation.speedOverGround'
     ORDER BY received_timestamp
     LIMIT 5`
  );
  const spotRows = spotResult.getRowObjects() as Array<{
    received_timestamp: string;
    value: number;
    source: string;
  }>;
  if (spotRows.length === 0) {
    fail('No navigation.speedOverGround rows found');
    failures++;
  } else {
    for (const r of spotRows) {
      console.log(
        `  ${r.received_timestamp}  value=${r.value.toFixed(4)}  src=${r.source}`
      );
    }
    pass(`${spotRows.length} rows returned`);
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
