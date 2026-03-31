#!/usr/bin/env node
// Aggregate Hive-partitioned raw value Parquet files through tiers:
//   raw → 5s → 60s → 1h
//
// Must be run after convert_values.ts has written all raw tier files.
//
// Run with:
//   node --experimental-strip-types aggregate_values.ts \
//     --data-dir /path/to/signalk-data-dir

import { DuckDBInstance } from '@duckdb/node-api';
import { mkdirSync, readdirSync } from 'fs';
import { join, resolve } from 'path';

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(): { dataDir: string } {
  const args = process.argv.slice(2);
  let dataDir = '';
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--data-dir' && args[i + 1]) dataDir = resolve(args[++i]);
  }
  if (!dataDir) {
    console.error(
      'Usage: node --experimental-strip-types aggregate_values.ts --data-dir <dir>'
    );
    process.exit(1);
  }
  return { dataDir };
}

// ---------------------------------------------------------------------------
// Angular path allowlist
// ---------------------------------------------------------------------------

const ANGULAR_PATHS = new Set<string>([
  // Navigation headings and courses (rad)
  'navigation.headingMagnetic',
  'navigation.headingTrue',
  'navigation.headingTrueCalc',
  'navigation.courseOverGroundTrue',
  'navigation.courseOverGroundMagnetic',
  'navigation.magneticVariation',
  'navigation.courseGreatCircle.bearingTrackTrue',
  'navigation.courseGreatCircle.nextPoint.bearingTrue',
  // Wind angles (rad)
  'environment.wind.angleApparent',
  'environment.wind.angleTrueGround',
  'environment.wind.angleTrueWater',
  'environment.wind.directionGround',
  'environment.wind.directionMagnetic',
  'environment.wind.directionTrue',
  // Steering (rad)
  'steering.rudderAngle',
  // Non-standard derived paths present in this dataset
  'variation',
  'headingMag',
]);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CONTEXT_ENC = 'vessels__urn-mrn-imo-mmsi-230029970';

interface TierSpec {
  name: string;
  interval: string;
  srcTier: string;
  isRawSrc: boolean;
}

const TIERS: TierSpec[] = [
  { name: '5s',  interval: '5 seconds',    srcTier: 'raw', isRawSrc: true  },
  { name: '60s', interval: '60 seconds',   srcTier: '5s',  isRawSrc: false },
  { name: '1h',  interval: '3600 seconds', srcTier: '60s', isRawSrc: false },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function pad(n: number, w: number): string {
  return String(n).padStart(w, '0');
}

/** Decode a Hive-encoded path back to a SignalK path (__ → .) */
function decodePathEnc(pathEnc: string): string {
  return pathEnc.replace(/__/g, '.');
}

/** Convert (year, doy) to YYYY-MM-DD string. */
function dayOfYearToDateStr(year: number, doy: number): string {
  const d = new Date(Date.UTC(year, 0, 1) + (doy - 1) * 86_400_000);
  return d.toISOString().slice(0, 10);
}

interface Partition {
  pathEnc: string;
  pathStr: string;
  year: number;
  doy: number;
}

/** Walk tier=raw to enumerate all (pathEnc, year, doy) partitions. */
function findRawPartitions(dataDir: string): Partition[] {
  const rawBase = join(dataDir, 'tier=raw', `context=${CONTEXT_ENC}`);
  const results: Partition[] = [];

  for (const pathDir of readdirSync(rawBase)) {
    if (!pathDir.startsWith('path=')) continue;
    const pathEnc = pathDir.slice('path='.length);
    const pathStr = decodePathEnc(pathEnc);
    const pathBase = join(rawBase, pathDir);

    for (const yearDir of readdirSync(pathBase)) {
      if (!yearDir.startsWith('year=')) continue;
      const year = parseInt(yearDir.slice('year='.length), 10);
      const yearBase = join(pathBase, yearDir);

      for (const dayDir of readdirSync(yearBase)) {
        if (!dayDir.startsWith('day=')) continue;
        const doy = parseInt(dayDir.slice('day='.length), 10);
        results.push({ pathEnc, pathStr, year, doy });
      }
    }
  }

  return results.sort((a, b) => {
    if (a.pathEnc !== b.pathEnc) return a.pathEnc.localeCompare(b.pathEnc);
    if (a.year !== b.year) return a.year - b.year;
    return a.doy - b.doy;
  });
}

function buildTierDir(
  dataDir: string,
  tier: string,
  pathEnc: string,
  year: number,
  doy: number
): string {
  return join(
    dataDir,
    `tier=${tier}`,
    `context=${CONTEXT_ENC}`,
    `path=${pathEnc}`,
    `year=${year}`,
    `day=${pad(doy, 3)}`
  );
}

function buildAggFile(dir: string, dateStr: string): string {
  return join(dir, `data_${dateStr}_aggregated.parquet`);
}

// ---------------------------------------------------------------------------
// SQL builders
// ---------------------------------------------------------------------------

function rawTo5sSQL(
  rawGlob: string,
  outFile: string,
  isAngular: boolean
): string {
  const safeGlob = rawGlob.replace(/'/g, "''");
  const safeOut  = outFile.replace(/'/g, "''");

  if (isAngular) {
    return `
      COPY (
        SELECT
          time_bucket(INTERVAL '5 seconds', received_timestamp::TIMESTAMP) AS bucket_time,
          context, path,
          ATAN2(AVG(SIN(CAST(value AS DOUBLE))),
                AVG(COS(CAST(value AS DOUBLE))))  AS value_avg,
          NULL::DOUBLE                            AS value_min,
          NULL::DOUBLE                            AS value_max,
          COUNT(*)                                AS sample_count,
          AVG(SIN(CAST(value AS DOUBLE)))         AS value_sin_avg,
          AVG(COS(CAST(value AS DOUBLE)))         AS value_cos_avg,
          MIN(received_timestamp)                 AS first_timestamp,
          MAX(received_timestamp)                 AS last_timestamp
        FROM read_parquet('${safeGlob}', union_by_name=true)
        WHERE value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
        GROUP BY bucket_time, context, path
        ORDER BY bucket_time
      ) TO '${safeOut}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
    `;
  }

  return `
    COPY (
      SELECT
        time_bucket(INTERVAL '5 seconds', received_timestamp::TIMESTAMP) AS bucket_time,
        context, path,
        AVG(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
                 THEN CAST(value AS DOUBLE) END)  AS value_avg,
        MIN(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
                 THEN CAST(value AS DOUBLE) END)  AS value_min,
        MAX(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
                 THEN CAST(value AS DOUBLE) END)  AS value_max,
        COUNT(*)                                  AS sample_count,
        MIN(received_timestamp)                   AS first_timestamp,
        MAX(received_timestamp)                   AS last_timestamp
      FROM read_parquet('${safeGlob}', union_by_name=true)
      GROUP BY bucket_time, context, path
      ORDER BY bucket_time
    ) TO '${safeOut}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
  `;
}

function tierToTierSQL(
  srcFile: string,
  outFile: string,
  interval: string,
  isAngular: boolean
): string {
  const safeSrc = srcFile.replace(/'/g, "''");
  const safeOut = outFile.replace(/'/g, "''");

  if (isAngular) {
    return `
      COPY (
        SELECT
          time_bucket(INTERVAL '${interval}', src_bucket_time::TIMESTAMP) AS bucket_time,
          context, path,
          ATAN2(
            SUM(value_sin_avg * sample_count) / SUM(sample_count),
            SUM(value_cos_avg * sample_count) / SUM(sample_count)
          )                                               AS value_avg,
          NULL::DOUBLE                                    AS value_min,
          NULL::DOUBLE                                    AS value_max,
          SUM(sample_count)::BIGINT                       AS sample_count,
          SUM(value_sin_avg * sample_count) / SUM(sample_count) AS value_sin_avg,
          SUM(value_cos_avg * sample_count) / SUM(sample_count) AS value_cos_avg,
          MIN(first_timestamp)                            AS first_timestamp,
          MAX(last_timestamp)                             AS last_timestamp
        FROM (
          SELECT bucket_time AS src_bucket_time, context, path,
                 value_sin_avg, value_cos_avg, sample_count,
                 first_timestamp, last_timestamp
          FROM read_parquet('${safeSrc}', union_by_name=true)
        ) src
        GROUP BY 1, context, path
        ORDER BY 1
      ) TO '${safeOut}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
    `;
  }

  return `
    COPY (
      SELECT
        time_bucket(INTERVAL '${interval}', src_bucket_time::TIMESTAMP) AS bucket_time,
        context, path,
        SUM(value_avg * sample_count) / SUM(sample_count)  AS value_avg,
        MIN(value_min)                                      AS value_min,
        MAX(value_max)                                      AS value_max,
        SUM(sample_count)::BIGINT                          AS sample_count,
        MIN(first_timestamp)                               AS first_timestamp,
        MAX(last_timestamp)                                AS last_timestamp
      FROM (
        SELECT bucket_time AS src_bucket_time, context, path,
               value_avg, value_min, value_max, sample_count,
               first_timestamp, last_timestamp
        FROM read_parquet('${safeSrc}', union_by_name=true)
      ) src
      GROUP BY 1, context, path
      ORDER BY 1
    ) TO '${safeOut}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
  `;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const { dataDir } = parseArgs();

  console.log(`Data dir: ${dataDir}`);
  console.log('Scanning raw tier partitions…');

  const partitions = findRawPartitions(dataDir);
  console.log(`Found ${partitions.length.toLocaleString()} raw partitions.`);
  console.log();

  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();

  let done = 0;

  for (const { pathEnc, pathStr, year, doy } of partitions) {
    const dateStr = dayOfYearToDateStr(year, doy);
    const isAngular = ANGULAR_PATHS.has(pathStr);

    const rawDir = buildTierDir(dataDir, 'raw', pathEnc, year, doy);
    const rawGlob = join(rawDir, '*.parquet');

    // Tier chain: raw → 5s → 60s → 1h
    let prevFile = rawGlob;
    let isRawSrc = true;

    for (const tier of TIERS) {
      const outDir = buildTierDir(dataDir, tier.name, pathEnc, year, doy);
      mkdirSync(outDir, { recursive: true });
      const outFile = buildAggFile(outDir, dateStr);

      const sql = isRawSrc
        ? rawTo5sSQL(prevFile, outFile, isAngular)
        : tierToTierSQL(prevFile, outFile, tier.interval, isAngular);

      await con.runAndReadAll(sql);
      prevFile = outFile;
      isRawSrc = false;
    }

    done++;
    if (done % 100 === 0 || done === partitions.length) {
      const pct = ((done / partitions.length) * 100).toFixed(1);
      console.log(
        `  [${pad(done, 5)}/${partitions.length}]  ${pct}%` +
          `  ${dateStr}  ${pathStr}${isAngular ? '  [angular]' : ''}`
      );
    }
  }

  console.log();
  console.log('Aggregation complete (raw → 5s → 60s → 1h).');
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
