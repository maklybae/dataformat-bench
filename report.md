# Data Format Benchmark Results

Total records: 53,687,091

## Summary

| Format   | File Size   | Write Time   | Full Scan   | Filtered Read   | Aggregation   |
|----------|-------------|--------------|-------------|-----------------|---------------|
| AVRO     | 5.02 GB     | 11.14 min    | 2.58 min    | 1.82 min        | 1.78 min      |
| PARQUET  | 4.11 GB     | 7.90 min     | TIMEOUT     | 56.35 s         | 1.40 s        |
| PROTOBUF | 7.35 GB     | 8.50 min     | 1.61 min    | 34.06 s         | 33.94 s       |

## Relative Performance

| Format   | Size vs Best   | Write vs Best   | Full Scan vs Best   | Filtered vs Best   | Aggregate vs Best   |
|----------|----------------|-----------------|---------------------|--------------------|---------------------|
| AVRO     | +22.1%         | +41.0%          | +60.6%              | +220.8%            | +7528.4%            |
| PARQUET  | 0.0%           | 0.0%            | TIMEOUT             | +65.4%             | 0.0%                |
| PROTOBUF | +78.6%         | +7.6%           | 0.0%                | 0.0%               | +2329.4%            |


## Analysis

### File Size (Storage Efficiency)
- **Best:** PARQUET (4.11 GB)
- **Worst:** PROTOBUF (7.35 GB)
- PARQUET achieves 1.79x better compression than PROTOBUF

### Write Performance
- **Fastest:** PARQUET (7.90 min)

### Full Scan Performance
- **Fastest:** PROTOBUF (1.61 min)

### Filtered Read Performance
- **Fastest:** PROTOBUF (34.06 s)

### Aggregation Performance
- **Fastest:** PARQUET (1.40 s)