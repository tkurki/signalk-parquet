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

### Step 1: Read & validate source data

```
scripts/cassiopeia-parquet/convert.py
```

1. Open `stash-cassiopeia-data/cassiopeia_trackpoint.parquet` with PyArrow
2. Filter out rows where lat/lng are out of range (|lat| > 90 or |lng| > 180)
3. Decode `context` bytes → UTF-8 string
4. Decode `sourceRef` bytes → UTF-8 string
5. Reconstruct full ISO 8601 timestamp: `datetime.utcfromtimestamp(ts + millis/1000).isoformat() + 'Z'`

### Step 2: Transform columns

For each row, produce a target record:

| Target Column          | Source Expression |
|-----------------------|-------------------|
| `received_timestamp`  | ISO 8601 from `ts` + `millis` |
| `signalk_timestamp`   | same as `received_timestamp` (only one timestamp in source) |
| `context`             | `'vessels.' + context.decode('utf-8')` |
| `path`                | `'navigation.position'` (constant) |
| `value_latitude`      | `lat` |
| `value_longitude`     | `lng` |
| `source`              | `sourceRef.decode('utf-8')` |
| `source_label`        | `sourceRef.decode('utf-8')` |

### Step 3: Partition by day

Group transformed records by `(year, day_of_year)` derived from the timestamp.

### Step 4: Write Hive-partitioned Parquet files

For each `(year, day)` group:
1. Build target directory:
   ```
   {output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path=navigation__position/year={YYYY}/day={DDD}/
   ```
2. Sort records by `signalk_timestamp`
3. Write as a single Parquet file per day:
   `data_{YYYYMMDD}T000000.parquet`
4. Use Snappy compression (consistent with plugin's DuckDB consolidation)

### Step 5: Verify output

```
scripts/cassiopeia-parquet/verify.py
```

1. Glob all output `.parquet` files
2. Read each with DuckDB/PyArrow and validate:
   - Schema matches expected columns and types
   - `value_latitude` and `value_longitude` are within valid ranges
   - `signalk_timestamp` falls within the day indicated by the partition path
   - Total row count across all files matches source (minus filtered rows)
3. Spot-check: query a few days via DuckDB with Hive partitioning enabled to confirm partition pruning works

---

## File Layout

```
scripts/cassiopeia-parquet/
├── README.md              # Usage instructions
├── convert.py             # Main conversion script
├── verify.py              # Post-conversion verification
└── requirements.txt       # Python deps (pyarrow, pandas)
```

### Usage

```bash
# From project root, with venv activated:
cd scripts/cassiopeia-parquet

# Convert (writes to a configurable output directory)
python convert.py \
  --input ../../stash-cassiopeia-data/cassiopeia_trackpoint.parquet \
  --output /path/to/signalk-data-dir

# Verify
python verify.py --data-dir /path/to/signalk-data-dir
```

---

## Edge Cases & Decisions

| Issue | Decision |
|-------|----------|
| 137 rows with out-of-range lat/lng | Filter out, log count |
| No separate `received_timestamp` vs `signalk_timestamp` in source | Use same value for both |
| `quadkey` column | Drop (not used by plugin) |
| `sourceRef` has 7 distinct values | Map directly to `source` and `source_label` |
| Sub-second precision | Preserved via `millis` → ISO 8601 fractional seconds |
| Days with very few records (min 2) | Still create a partition file per day |
| `context` prefix | Source has bare `urn:mrn:imo:mmsi:...`; target needs `vessels.` prefix |
| Compression | Snappy (consistent with consolidate-parquet.sh) |
| `value` column | Omitted — exploded object files don't include it |
| days spanning year boundaries | Handled by computing day-of-year per UTC timestamp |

---

## Compatibility Verification

After conversion, the output should be queryable by the plugin's DuckDB-based
History API. Verification query (from `verify.py`):

```sql
SELECT signalk_timestamp, value_latitude, value_longitude
FROM read_parquet(
  '{output_dir}/tier=raw/context=vessels__urn-mrn-imo-mmsi-230029970/path=navigation__position/year=*/day=*/*.parquet',
  hive_partitioning=true
)
ORDER BY signalk_timestamp
LIMIT 10;
```
