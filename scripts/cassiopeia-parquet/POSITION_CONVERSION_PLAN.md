# Cassiopeia Trackpoint Parquet Conversion Plan

## Overview

Convert `cassiopeia_trackpoint.parquet` (a flat file containing ~6.65M `navigation.position` records from vessel `urn:mrn:imo:mmsi:230029970`) into the Hive-partitioned, exploded-column Parquet format used by the signalk-parquet plugin.

All conversion code lives under `scripts/cassiopeia-parquet/`.

---

## Source Data Format

**File:** `stash-cassiopeia-data/cassiopeia_trackpoint.parquet`  
**Rows:** 6,653,954  
**Time range:** 2019-06-26 → 2023-06-27 (96 unique days)  
**Context:** single vessel `urn:mrn:imo:mmsi:230029970`

### Schema

| Column      | Arrow Type | Description |
|-------------|-----------|-------------|
| `ts`        | `INT64`   | Unix epoch seconds |
| `millis`    | `UINT16`  | Sub-second milliseconds (0–999) |
| `context`   | `BINARY`  | Vessel identifier bytes, e.g. `b'urn:mrn:imo:mmsi:230029970'` |
| `sourceRef` | `BINARY`  | Source reference bytes, e.g. `b'can0.c07891002fb5847c'` |
| `lat`       | `DOUBLE`  | Latitude in decimal degrees |
| `lng`       | `DOUBLE`  | Longitude in decimal degrees |
| `quadkey`   | `UINT64`  | Quadkey tile index (not used in target) |

### Notes
- Timestamp is split across `ts` (seconds) and `millis` (fractional).
  Full millisecond epoch = `ts * 1000 + millis`.
- 137 rows (~0.002%) have out-of-range lat/lng (absolute value > 90/180); these should be **filtered out** during conversion.
- `context` and `sourceRef` are raw bytes, need decoding to UTF-8 strings.

---

## Target Data Format

### Hive Partition Structure

```
{outputDirectory}/
  tier=raw/
    context=vessels__urn-mrn-imo-mmsi-230029970/
      path=navigation__position/
        year=2019/
          day=177/
            data_20190626T160618.parquet
          day=178/
            ...
        year=2020/
          ...
        year=2023/
          ...
```

Partition key encoding rules (from `HivePathBuilder`):
- **tier:** always `raw` for unconsolidated source data
- **context:** dots → `__`, colons → `-`
  - `vessels.urn:mrn:imo:mmsi:230029970` → `vessels__urn-mrn-imo-mmsi-230029970`
- **path:** dots → `__`
  - `navigation.position` → `navigation__position`
- **year:** 4-digit UTC year
- **day:** 3-digit zero-padded UTC day-of-year (1–366)

### Per-File Parquet Schema (exploded object format)

For `navigation.position`, the value is an object `{latitude, longitude}`
which gets "exploded" into separate `value_*` columns. Each file contains
one day's data for one context+path.

| Column                | Parquet Type   | Optional | Description |
|-----------------------|---------------|----------|-------------|
| `received_timestamp`  | `UTF8`        | yes      | ISO 8601 string — when the server received the delta |
| `signalk_timestamp`   | `UTF8`        | yes      | ISO 8601 string — timestamp from the SignalK delta |
| `context`             | `UTF8`        | yes      | Vessel context, e.g. `vessels.urn:mrn:imo:mmsi:230029970` |
| `path`                | `UTF8`        | yes      | SignalK path: `navigation.position` |
| `value_latitude`      | `DOUBLE`      | yes      | Latitude component of position |
| `value_longitude`     | `DOUBLE`      | yes      | Longitude component of position |
| `value_json`          | –             | –        | **Omitted** (not written to Parquet by schema service) |
| `source`              | `UTF8`        | yes      | Source reference string, e.g. `can0.c07891002fb5847c` |
| `source_label`        | `UTF8`        | yes      | Source label (= sourceRef from source) |

**Key differences from source:**
- Timestamps are ISO 8601 strings, not epoch integers
- Lat/Lng become `value_latitude` / `value_longitude` (exploded object fields)
- `context` is prefixed with `vessels.` and is a UTF-8 string
- `path` column added (always `navigation.position`)
- `quadkey` is dropped
- `value` column (scalar) is absent (null/omitted for object-type paths)

---

## Conversion Steps

### Step 1: Enumerate day partitions with DuckDB

```
scripts/cassiopeia-parquet/convert_position.ts
```

Open the source file with DuckDB and query the distinct `(year, day_of_year)` partitions present, applying the lat/lng validity filter up-front:

```sql
SELECT
    CAST(ts / 86400 AS INTEGER) AS day_epoch
FROM read_parquet('stash-cassiopeia-data/cassiopeia_trackpoint.parquet')
WHERE lat BETWEEN -90 AND 90
  AND lng BETWEEN -180 AND 180
GROUP BY 1
ORDER BY 1
```

### Step 2: Extract, transform, and write one day at a time

For each `day_epoch`, use a single DuckDB `COPY (...) TO` statement that performs
all column transformations in SQL and writes directly to a Parquet file — no
intermediate in-memory table required:

```sql
COPY (
  SELECT
    strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
        || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z'
        AS received_timestamp,
    strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
        || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z'
        AS signalk_timestamp,
    'vessels.' || CAST(context AS VARCHAR)  AS context,
    'navigation.position'                   AS path,
    lat                                     AS value_latitude,
    lng                                     AS value_longitude,
    CAST(sourceRef AS VARCHAR)              AS source,
    CAST(sourceRef AS VARCHAR)              AS source_label
  FROM read_parquet('<input_file>')
  WHERE lat BETWEEN -90 AND 90
    AND lng BETWEEN -180 AND 180
    AND CAST(ts / 86400 AS INTEGER) = <day_epoch>
  ORDER BY ts, millis
) TO '<output_file>' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
```

| Target Column          | Source Expression |
|-----------------------|-------------------|
| `received_timestamp`  | ISO 8601 built from `ts + millis/1000.0` |
| `signalk_timestamp`   | same as `received_timestamp` |
| `context`             | `'vessels.' \|\| CAST(context AS VARCHAR)` |
| `path`                | `'navigation.position'` (constant) |
| `value_latitude`      | `lat` |
| `value_longitude`     | `lng` |
| `source`              | `CAST(sourceRef AS VARCHAR)` |
| `source_label`        | `CAST(sourceRef AS VARCHAR)` |

### Step 3: Write Hive-partitioned Parquet files

The `COPY TO` in Step 2 writes directly to the target file. Before running the
query, build the output path and create the directory:
1. Build target directory:
   ```
   {output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path=navigation__position/year={YYYY}/day={DDD}/
   ```
2. Output file name: `data_{YYYYMMDD}T000000.parquet`
3. Snappy compression is specified in the `COPY TO` clause — consistent with the plugin's DuckDB consolidation

### Step 4: Aggregation — not applicable

`navigation.position` is an **object-type path** and is intentionally excluded from
aggregation. The plugin's `aggregateTier` checks whether the source Parquet files
contain a `value` column (required for DuckDB `AVG(CAST(value AS DOUBLE))`). Position
files have `value_latitude` and `value_longitude` columns but no `value` column, so
`aggregateTier` returns immediately with 0 records — the path stays `tier=raw` only.
This is by design: there is no meaningful scalar average of a lat/lng pair.

The batch conversion script therefore **skips aggregation entirely** for position data,
consistent with the plugin's behaviour.

### Step 5: Verify output

```
scripts/cassiopeia-parquet/verify_position.ts
```

1. Use DuckDB to count total rows across all output files and compare against source (minus filtered rows)
2. Validate schema: columns and types match expectation
3. Check `value_latitude` / `value_longitude` stay within valid ranges
4. Verify `signalk_timestamp` falls within the day indicated by the partition path
5. Confirm partition pruning works via Hive-aware DuckDB query

## Implementation Strategy

DuckDB handles all decoding of `BINARY` columns automatically when casting to `VARCHAR`. The conversion iterates over ~96 day partitions, writing each directly to a Parquet file via DuckDB's `COPY TO` — no intermediate in-memory table or separate Parquet writer is needed. With a maximum of ~607K rows on the busiest day, each partition is well within DuckDB's streaming write capacity.

Scripts are written in TypeScript and executed directly with `node --strip-types` (Node ≥ 22.6.0), which strips type annotations at runtime without a compile step. All dependencies (`@duckdb/node-api`) are already present in the project's `node_modules`.

```typescript
// Pseudocode (convert_position.ts)
import { DuckDBInstance } from '@duckdb/node-api';
import { mkdirSync } from 'fs';
import { join } from 'path';

const instance = await DuckDBInstance.create(':memory:');
const con = await instance.connect();

// Step 1: enumerate partitions
const partResult = await con.runAndReadAll(`
  SELECT CAST(ts / 86400 AS INTEGER)
  FROM read_parquet('${inputFile}')
  WHERE lat BETWEEN -90 AND 90 AND lng BETWEEN -180 AND 180
  GROUP BY 1 ORDER BY 1
`);
const dayEpochs = partResult.getRows().map((r) => r[0] as number);

for (const dayEpoch of dayEpochs) {
  const d = epochDayToDate(dayEpoch);
  const outDir = buildOutputDir(outputDir, d);
  mkdirSync(outDir, { recursive: true });
  const outFile = join(outDir, buildFilename(d));

  // Steps 2 + 3: transform and write in a single COPY TO
  await con.run(`
    COPY (
      SELECT
        strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
            || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z' AS received_timestamp,
        strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
            || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z' AS signalk_timestamp,
        'vessels.' || CAST(context AS VARCHAR) AS context,
        'navigation.position' AS path,
        lat AS value_latitude,
        lng AS value_longitude,
        CAST(sourceRef AS VARCHAR) AS source,
        CAST(sourceRef AS VARCHAR) AS source_label
      FROM read_parquet('${inputFile}')
      WHERE lat BETWEEN -90 AND 90 AND lng BETWEEN -180 AND 180
        AND CAST(ts / 86400 AS INTEGER) = ${dayEpoch}
      ORDER BY ts, millis
    ) TO '${outFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
  `);
}
```

---

## File Layout

```
scripts/cassiopeia-parquet/
├── POSITION_CONVERSION_PLAN.md  # This document
├── VALUE_CONVERSION_PLAN.md     # Value conversion plan
├── README.md                    # Usage instructions
├── convert_position.ts          # Position conversion script
├── verify_position.ts           # Position verification
├── convert_values.ts            # Value conversion script
├── verify_values.ts             # Value verification
```

No extra dependencies — `@duckdb/node-api` is already in the project's
`node_modules`. Scripts run directly with `node --strip-types` (Node ≥ 22.6.0),
which strips TypeScript type annotations without a compile step.

### Usage

```bash
# From project root:
cd scripts/cassiopeia-parquet

# Convert (writes to a configurable output directory)
node --strip-types convert_position.ts \
  --input ../../stash-cassiopeia-data/cassiopeia_trackpoint.parquet \
  --output /path/to/signalk-data-dir

# Verify
node --strip-types verify_position.ts --data-dir /path/to/signalk-data-dir
```

---

## Edge Cases & Decisions

| Issue | Decision |
|-------|----------|
| 137 rows with out-of-range lat/lng | Filtered in DuckDB `WHERE lat BETWEEN -90 AND 90 AND lng BETWEEN -180 AND 180`; count logged |
| `BINARY` columns need decoding | DuckDB `CAST(... AS VARCHAR)` decodes UTF-8 bytes automatically |
| No separate `received_timestamp` vs `signalk_timestamp` | Use same value for both |
| `quadkey` column | Excluded from `SELECT` — not present in target schema |
| `sourceRef` has 7 distinct values | Cast directly to `source` and `source_label` |
| Sub-second precision | Preserved: `millis` appended as `.NNN` in ISO 8601 string in SQL |
| Days with very few records (min 2) | Still create a partition file per day |
| `context` prefix | Prepend `'vessels.'` in SQL: `'vessels.' \|\| CAST(context AS VARCHAR)` |
| Compression | Snappy (consistent with consolidate-parquet.sh) |
| `value` column | Omitted from `SELECT` — exploded object files don't include it |
| Days spanning year boundaries | DuckDB `ts / 86400` grouping naturally respects UTC day boundaries |

---

## Compatibility Verification

After conversion, the output should be queryable by the plugin's DuckDB-based
History API. Verification queries (from `verify_position.ts`):

```sql
-- Row count verification (should equal source minus 137 filtered rows)
SELECT COUNT(*) as total_rows
FROM read_parquet(
  '{output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path=navigation__position/year=*/day=*/*.parquet',
  hive_partitioning=true
);

-- Spot-check: first 10 records in order
SELECT signalk_timestamp, value_latitude, value_longitude, source
FROM read_parquet(
  '{output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path=navigation__position/year=*/day=*/*.parquet',
  hive_partitioning=true
)
ORDER BY signalk_timestamp
LIMIT 10;
```
