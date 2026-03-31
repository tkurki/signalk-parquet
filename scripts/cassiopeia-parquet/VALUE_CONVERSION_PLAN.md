# Cassiopeia Value Parquet Conversion Plan

## Overview

Convert `cassiopeia_value.parquet` (a flat file containing ~138.8M scalar value records across 175 SignalK paths from vessel `urn:mrn:imo:mmsi:230029970`) into the Hive-partitioned Parquet format used by the signalk-parquet plugin.

All conversion code lives under `scripts/cassiopeia-parquet/`.

---

## Source Data Format

**File:** `stash-cassiopeia-data/cassiopeia_value.parquet`  
**Rows:** 138,816,821  
**Time range:** 2019-06-25 → 2023-06-27  
**Context:** single vessel `urn:mrn:imo:mmsi:230029970`  
**Distinct paths:** 175  
**Distinct sources:** 53  

### Schema

| Column      | Arrow Type | Description |
|-------------|-----------|-------------|
| `ts`        | `INT64`   | Unix epoch seconds |
| `millis`    | `UINT16`  | Sub-second milliseconds (0–999) |
| `context`   | `BINARY`  | Vessel identifier bytes: `b'urn:mrn:imo:mmsi:230029970'` |
| `sourceRef` | `BINARY`  | Source reference bytes, e.g. `b'can0.c07891002fb5847c'` |
| `path`      | `BINARY`  | SignalK path bytes, e.g. `b'navigation.speedOverGround'` |
| `value`     | `FLOAT`   | 32-bit float scalar value (no NaN or Inf present) |

### Notes
- All values are scalar floats — no object/struct values (position is in the separate trackpoint file)
- `value` column is `FLOAT` (32-bit); target promotes to `DOUBLE` (64-bit) for consistency with plugin schema
- No NaN or Inf values present; no NULL values
- Timestamp is split across `ts` (seconds) and `millis` (fractional)

### Path Categories (175 paths)

| Category       | # Paths | Example Paths |
|----------------|---------|---------------|
| `electrical`   | 84      | `electrical.batteries.288.voltage`, `electrical.solar.260.panelPower` |
| `navigation`   | 28      | `navigation.speedOverGround`, `navigation.headingMagnetic` |
| `vhfdata`      | 27      | `vhfdata.nearest.0.distance`, `vhfdata.nearest.vts.relativeBearing` |
| `environment`  | 16      | `environment.wind.speedApparent`, `environment.water.temperature` |
| `tanks`        | 9       | `tanks.fuel.20.currentLevel` |
| `propulsion`   | 6       | `propulsion.1.coolantTemperature` |
| `design`       | 2       | `design.airHeight`, `design.beam` |
| `steering`     | 1       | `steering.rudderAngle` |
| `headingMag`   | 1       | Non-standard path (likely plugin-derived) |
| `variation`    | 1       | Non-standard path (likely plugin-derived) |

### Top 20 Paths by Row Count

| Path | Rows | Date Range |
|------|------|------------|
| `electrical.solar.260.panelVoltage` | 10,271,071 | 2019-06-25 → 2022-10-12 |
| `electrical.solar.258.panelVoltage` | 10,040,775 | 2019-06-25 → 2022-10-12 |
| `electrical.solar.260.panelCurrent` | 7,355,070 | 2019-06-25 → 2021-10-12 |
| `electrical.solar.258.panelCurrent` | 6,655,827 | 2019-06-25 → 2021-10-12 |
| `electrical.batteries.288.power` | 6,371,937 | 2019-06-25 → 2022-10-12 |
| `electrical.venus.totalPanelPower` | 5,945,708 | 2019-06-25 → 2023-06-27 |
| `electrical.batteries.288.current` | 5,021,730 | 2019-06-25 → 2022-10-12 |
| `electrical.venus.totalPanelCurrent` | 4,014,824 | 2019-06-25 → 2023-06-27 |
| `electrical.batteries.288.capacity.timeRemaining` | 3,811,961 | 2019-06-25 → 2022-10-12 |
| `electrical.solar.260.voltage` | 3,796,796 | 2019-06-25 → 2022-10-12 |
| `electrical.solar.258.voltage` | 3,343,601 | 2019-06-25 → 2022-10-12 |
| `environment.wind.speedApparent` | 3,255,425 | 2019-06-26 → 2023-06-27 |
| `environment.wind.angleApparent` | 3,216,154 | 2019-06-26 → 2023-06-27 |
| `electrical.batteries.288.voltage` | 2,518,941 | 2019-06-25 → 2022-10-12 |
| `electrical.solar.260.current` | 2,408,824 | 2019-06-25 → 2022-10-12 |
| `navigation.magneticVariation` | 2,374,661 | 2019-06-26 → 2023-06-27 |
| `electrical.solar.258.current` | 2,324,571 | 2019-06-25 → 2022-10-12 |
| `electrical.solar.260.panelPower` | 2,160,468 | 2019-06-25 → 2022-10-12 |
| `environment.water.temperature` | 1,976,109 | 2019-06-26 → 2023-06-27 |
| `electrical.solar.258.panelPower` | 1,966,694 | 2019-06-25 → 2022-10-12 |

### Source Refs (53 distinct)

Diverse sensor sources including NMEA2000 CAN bus (`can0.*`, `actisense.*`), Victron Energy (`venus.*`), Calypso wind sensor (`calypso.*`), 1-Wire temperature sensors (`1w.*`), autopilot (`autopilot.*`), derived data (`derived-data`, `nodeRedCalc`), and VHF data (`vhfinfo`).

---

## Target Data Format

### Hive Partition Structure

Each path gets its own partition tree. For 175 paths, the output looks like:

```
{outputDirectory}/
  tier=raw/
    context=vessels__urn-mrn-imo-mmsi-230029970/
      path=navigation__speedOverGround/
        year=2019/
          day=177/
            data_20190626T000000.parquet
          day=178/
            ...
        year=2023/
          ...
      path=electrical__batteries__288__voltage/
        year=2019/
          ...
      path=environment__wind__speedApparent/
        ...
      ... (175 path partitions)
```

Partition key encoding (from `HivePathBuilder`):
- **tier:** always `raw`
- **context:** `vessels.urn:mrn:imo:mmsi:230029970` → `vessels__urn-mrn-imo-mmsi-230029970`
- **path:** dots → `__` (e.g. `navigation.speedOverGround` → `navigation__speedOverGround`)
- **year:** 4-digit UTC year
- **day:** 3-digit zero-padded UTC day-of-year (1–366)

### Per-File Parquet Schema (scalar value format)

These are scalar-value paths, so they use the standard `value` column
(not exploded `value_*` columns like `navigation.position`).

| Column                | Parquet Type   | Optional | Description |
|-----------------------|---------------|----------|-------------|
| `received_timestamp`  | `UTF8`        | yes      | ISO 8601 string |
| `signalk_timestamp`   | `UTF8`        | yes      | ISO 8601 string |
| `context`             | `UTF8`        | yes      | `vessels.urn:mrn:imo:mmsi:230029970` |
| `path`                | `UTF8`        | yes      | SignalK path (e.g. `navigation.speedOverGround`) |
| `value`               | `DOUBLE`      | yes      | Scalar numeric value (promoted from FLOAT to DOUBLE) |
| `source`              | `UTF8`        | yes      | Source reference string |
| `source_label`        | `UTF8`        | yes      | Source label (= sourceRef) |

**Key differences from source:**
- Timestamps become ISO 8601 strings (not epoch integers)
- `value` promoted from `FLOAT` (32-bit) to `DOUBLE` (64-bit)
- `context` prefixed with `vessels.` and decoded to UTF-8 string
- `source` and `source_label` decoded from bytes
- One file per (path, day) combination, sorted by timestamp

---

## Conversion Steps

### Step 1: Read source data with DuckDB

Using DuckDB for efficient streaming of the 138.8M row file without loading
everything into memory.

```
scripts/cassiopeia-parquet/convert_values.ts
```

1. Open `stash-cassiopeia-data/cassiopeia_value.parquet` with DuckDB
2. Process by `(path, date)` groups to keep memory bounded
3. Decode `context` and `sourceRef` bytes to UTF-8 strings

### Step 2: Transform columns

For each row, produce a target record:

| Target Column          | Source Expression |
|-----------------------|-------------------|
| `received_timestamp`  | ISO 8601 from `ts + millis/1000` |
| `signalk_timestamp`   | same as `received_timestamp` |
| `context`             | `'vessels.' + decode(context)` |
| `path`                | `decode(path)` (as-is after decoding) |
| `value`               | `CAST(value AS DOUBLE)` |
| `source`              | `decode(sourceRef)` |
| `source_label`        | `decode(sourceRef)` |

### Step 3: Partition by (path, day)

Group transformed records by `(path, year, day_of_year)`.

This yields approximately **175 paths × variable days per path** partition files. 
The largest paths (e.g. `electrical.solar.260.panelVoltage`) span ~1,200+ days;
the smallest paths (`design.beam`) have only 1 day.

### Step 4: Write Hive-partitioned Parquet files

For each `(path, year, day)` group:
1. Sanitize path for directory name: dots → `__`
2. Build target directory:
   ```
   {output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path={sanitized_path}/year={YYYY}/day={DDD}/
   ```
3. Sort records by `signalk_timestamp`
4. Write as a single Parquet file per day:
   `data_{YYYYMMDD}T000000.parquet`
5. Use Snappy compression

### Step 5: Aggregate to higher tiers

After all raw files are written, run the same aggregation logic the plugin uses
(`aggregateDate` → `aggregateTier`), chained through the full tier hierarchy
`raw → 5s → 60s → 1h`, for every day that has raw data.

#### Aggregated-tier file schema

Aggregated files use a different schema from raw:

| Column             | Type      | Scalar paths | Angular paths |
|--------------------|-----------|-------------|---------------|
| `bucket_time`      | TIMESTAMP | time bucket start | time bucket start |
| `context`          | VARCHAR   | ✓ | ✓ |
| `path`             | VARCHAR   | ✓ | ✓ |
| `value_avg`        | DOUBLE    | arithmetic mean | ATAN2(AVG(SIN), AVG(COS)) |
| `value_min`        | DOUBLE    | minimum | NULL |
| `value_max`        | DOUBLE    | maximum | NULL |
| `sample_count`     | BIGINT    | ✓ | ✓ |
| `value_sin_avg`    | DOUBLE    | absent | AVG(SIN(value)) |
| `value_cos_avg`    | DOUBLE    | absent | AVG(COS(value)) |
| `first_timestamp`  | VARCHAR   | ✓ | ✓ |
| `last_timestamp`   | VARCHAR   | ✓ | ✓ |

Output filename per day: `data_{YYYY-MM-DD}_aggregated.parquet` inside the same
hive directory structure, e.g.:
```
tier=5s/context=vessels__urn-mrn-imo-mmsi-230029970/path=navigation__speedOverGround/year=2023/day=177/
  data_2023-06-26_aggregated.parquet
```

#### Angular vs scalar path detection

The plugin calls `app.getMetadata(path)` at runtime to check `units === 'rad'`.
The batch script has no running SignalK server, so uses a static allowlist derived
from the SignalK specification for all angular paths present in this dataset:

```typescript
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
```

#### DuckDB aggregation queries (identical to plugin)

**Scalar path, `raw → 5s`:**
```sql
COPY (
  SELECT
    time_bucket(INTERVAL '5 seconds', received_timestamp::TIMESTAMP) AS bucket_time,
    context, path,
    AVG(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
            THEN CAST(value AS DOUBLE) END) AS value_avg,
    MIN(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
            THEN CAST(value AS DOUBLE) END) AS value_min,
    MAX(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
            THEN CAST(value AS DOUBLE) END) AS value_max,
    COUNT(*)                                AS sample_count,
    MIN(received_timestamp)                 AS first_timestamp,
    MAX(received_timestamp)                 AS last_timestamp
  FROM read_parquet([<raw_files>], union_by_name=true)
  GROUP BY bucket_time, context, path
  ORDER BY bucket_time
) TO '<output>' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
```

**Angular path, `raw → 5s`:**
```sql
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
  FROM read_parquet([<raw_files>], union_by_name=true)
  WHERE value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
  GROUP BY bucket_time, context, path
  ORDER BY bucket_time
) TO '<output>' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
```

**Tier-to-tier re-aggregation (e.g. `5s → 60s`), scalar:**
```sql
COPY (
  SELECT
    time_bucket(INTERVAL '60 seconds', src_bucket_time::TIMESTAMP) AS bucket_time,
    context, path,
    SUM(value_avg * sample_count) / SUM(sample_count) AS value_avg,
    MIN(value_min)                                    AS value_min,
    MAX(value_max)                                    AS value_max,
    SUM(sample_count)::BIGINT                         AS sample_count,
    MIN(first_timestamp)                              AS first_timestamp,
    MAX(last_timestamp)                               AS last_timestamp
  FROM (SELECT bucket_time AS src_bucket_time, context, path,
               value_avg, value_min, value_max, sample_count,
               first_timestamp, last_timestamp
        FROM read_parquet([<5s_files>], union_by_name=true)) src
  GROUP BY 1, context, path
  ORDER BY 1
) TO '<output>' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
```

**Tier-to-tier re-aggregation, angular** (uses stored `value_sin_avg`/`value_cos_avg`
for correct weighted re-composition):
```sql
COPY (
  SELECT
    time_bucket(INTERVAL '60 seconds', src_bucket_time::TIMESTAMP) AS bucket_time,
    context, path,
    ATAN2(
      SUM(value_sin_avg * sample_count) / SUM(sample_count),
      SUM(value_cos_avg * sample_count) / SUM(sample_count)
    )                                              AS value_avg,
    NULL::DOUBLE                                   AS value_min,
    NULL::DOUBLE                                   AS value_max,
    SUM(sample_count)::BIGINT                      AS sample_count,
    SUM(value_sin_avg * sample_count) / SUM(sample_count) AS value_sin_avg,
    SUM(value_cos_avg * sample_count) / SUM(sample_count) AS value_cos_avg,
    MIN(first_timestamp)                           AS first_timestamp,
    MAX(last_timestamp)                            AS last_timestamp
  FROM (SELECT bucket_time AS src_bucket_time, context, path,
               value_sin_avg, value_cos_avg, sample_count,
               first_timestamp, last_timestamp
        FROM read_parquet([<5s_files>], union_by_name=true)) src
  GROUP BY 1, context, path
  ORDER BY 1
) TO '<output>' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
```

The same `60s → 1h` query applies with `INTERVAL '3600 seconds'`.

### Step 6: Verify output

```
scripts/cassiopeia-parquet/verify_values.ts
```

1. Glob all output `.parquet` files
2. Validate per-path total row count matches source
3. Schema check: columns and types are correct for both raw and aggregated tiers
4. Verify timestamps fall within expected day partition
5. Spot-check specific paths via DuckDB with Hive partition pruning
6. Confirm aggregated tiers have progressively fewer rows per tier

---

## Implementation Strategy: Streaming with DuckDB

Given 138.8M rows, the converter should **not** load the entire file into memory.
Instead, use DuckDB to:

1. Query the distinct `(path, date)` groups first
2. For each group, transform and write rows directly via `COPY (...) TO (FORMAT PARQUET)` — no intermediate in-memory table required
3. No extra dependencies: `@duckdb/node-api` is already in the project's `node_modules`

Scripts are written in TypeScript and executed directly with `node --strip-types`
(Node ≥ 22.6.0), which strips type annotations at runtime without a compile step.

```typescript
// Pseudocode (convert_values.ts)
import { DuckDBInstance } from '@duckdb/node-api';
import { mkdirSync } from 'fs';
import { join } from 'path';

const instance = await DuckDBInstance.create(':memory:');
const con = await instance.connect();

// Get all distinct (path, date) partitions
const partResult = await con.runAndReadAll(`
  SELECT CAST(path AS VARCHAR) AS p,
         CAST(ts / 86400 AS INTEGER) AS day_epoch
  FROM read_parquet('${inputFile}')
  GROUP BY 1, 2
  ORDER BY 1, 2
`);
const partitions = partResult.getRows() as [string, number][];

for (const [pathStr, dayEpoch] of partitions) {
  const outDir = buildOutputDir(outputDir, pathStr, dayEpoch);
  mkdirSync(outDir, { recursive: true });
  const outFile = join(outDir, buildFilename(dayEpoch));

  // Extract, transform, and write in a single COPY TO
  await con.run(`
    COPY (
      SELECT
        strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
            || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z' AS received_timestamp,
        strftime(to_timestamp(ts + millis / 1000.0), '%Y-%m-%dT%H:%M:%S.')
            || lpad(CAST(millis % 1000 AS VARCHAR), 3, '0') || 'Z' AS signalk_timestamp,
        'vessels.' || CAST(context AS VARCHAR) AS context,
        CAST(path AS VARCHAR) AS path,
        CAST(value AS DOUBLE) AS value,
        CAST(sourceRef AS VARCHAR) AS source,
        CAST(sourceRef AS VARCHAR) AS source_label
      FROM read_parquet('${inputFile}')
      WHERE CAST(path AS VARCHAR) = '${pathStr.replace(/'/g, "''")}'
        AND CAST(ts / 86400 AS INTEGER) = ${dayEpoch}
      ORDER BY ts, millis
    ) TO '${outFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
  `);
}
```

This keeps memory usage minimal: DuckDB streams each partition directly to disk.

---

## File Layout

```
scripts/cassiopeia-parquet/
├── POSITION_CONVERSION_PLAN.md  # Position conversion plan
├── VALUE_CONVERSION_PLAN.md     # This document
├── README.md                    # Usage instructions
├── convert_position.ts          # Position (trackpoint) conversion script
├── verify_position.ts           # Position verification
├── convert_values.ts            # Value conversion script
├── aggregate_values.ts          # Aggregation script (raw → 5s → 60s → 1h)
├── verify_values.ts             # Value verification
```

No extra dependencies — `@duckdb/node-api` is already in the project's
`node_modules`. Scripts run directly with `node --strip-types` (Node ≥ 22.6.0),
which strips TypeScript type annotations without a compile step.

### Usage

```bash
# From project root:
cd scripts/cassiopeia-parquet

# Step 1: Convert raw values
node --strip-types convert_values.ts \
  --input ../../stash-cassiopeia-data/cassiopeia_value.parquet \
  --output /path/to/signalk-data-dir

# Step 2: Aggregate all tiers (raw → 5s → 60s → 1h)
node --strip-types aggregate_values.ts \
  --data-dir /path/to/signalk-data-dir

# Step 3: Verify
node --strip-types verify_values.ts --data-dir /path/to/signalk-data-dir
```

---

## Edge Cases & Decisions

| Issue | Decision |
|-------|----------|
| 138.8M rows won't fit in memory | Use DuckDB `COPY TO` for streaming partition-at-a-time extraction — no in-memory table |
| `value` is FLOAT (32-bit) | Promote to DOUBLE (64-bit) to match plugin schema |
| No separate received vs signalk timestamp | Use same value for both |
| Non-standard paths (`headingMag`, `variation`) | Include as-is — the plugin handles arbitrary paths |
| Paths with very few rows (e.g. `design.beam` = 3 rows) | Still create partition files; consistent with plugin behavior |
| vhfdata paths (27 paths, all on same dates) | Convert normally; these are valid SignalK-adjacent data |
| Large path/day groups (solar panels: ~10K rows/day) | Well within single-file memory limits |
| Source `sourceRef` bytes contain dots and long strings | Decode as-is to UTF-8; no sanitization needed for column values |
| Multiple `sourceRef` values per path | Preserved in `source` column; plugin handles multi-source |
| Compression | Snappy (consistent with plugin consolidation) |
| `millis` sub-second precision | Preserved via ISO 8601 fractional seconds (`.NNN`) |
| Some paths span 4 years, others just 1 day | Partition structure accommodates both naturally |
| Angular path detection (no running SignalK server) | Static allowlist of 17 paths from SignalK spec (`units === 'rad'`) |
| Tier-to-tier re-aggregation requires stored sin/cos for angular | 5s tier writes `value_sin_avg`/`value_cos_avg`; used by 60s and 1h aggregation |
| Aggregation overwrites existing files | Consistent with plugin behaviour (`aggregateTier` does not check for existing output) |

---

## Scale Estimates

| Metric | Value |
|--------|-------|
| Source rows | 138,816,821 |
| Distinct paths | 175 |
| Distinct (path, day) raw partitions | ~4,000–6,000 (estimated) |
| Largest single raw day/path | ~600K rows |
| Aggregated tiers | 3 (`5s`, `60s`, `1h`) — one output file per path per day per tier |
| Expected raw file count | ~4,000–6,000 `.parquet` files |
| Expected aggregated file count | ~12,000–18,000 `.parquet` files (3× raw) |
| Expected total output size | ~3–6 GB (raw + all aggregated tiers, Snappy compressed) |
| Angular paths (vector averaging) | 17 (headings, courses, wind angles, rudder) |

---

## Compatibility Verification

After conversion, all paths should be queryable by the plugin's DuckDB-based
History API. Example verification query:

```sql
-- Query a specific scalar path with Hive partitioning
SELECT signalk_timestamp, value
FROM read_parquet(
  '{output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path=navigation__speedOverGround/year=*/day=*/*.parquet',
  hive_partitioning=true
)
WHERE signalk_timestamp >= '2023-06-01T00:00:00Z'
  AND signalk_timestamp < '2023-06-02T00:00:00Z'
ORDER BY signalk_timestamp
LIMIT 10;
```

```sql
-- Verify partition pruning works across all paths
SELECT path, COUNT(*) as cnt
FROM read_parquet(
  '{output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path=*/year=*/day=*/*.parquet',
  hive_partitioning=true
)
GROUP BY path
ORDER BY cnt DESC;
```
