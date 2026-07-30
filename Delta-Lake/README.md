# Delta Lake

Notebooks and SQL demonstrating Delta Lake storage features: performance layout,
schema evolution, semi-structured types, cloning, and interoperability with Apache Iceberg.

See also [`features/`](features/) for focused deep-dives (SQL scripting, query tags,
vacuum guardrails, type widening on streams, variant shredding, automatic liquid clustering).

## Numbered walkthroughs

| File | Type | Description |
|------|------|-------------|
| `01 - Liquid Clustering.py` | Python | Liquid clustering basics — `CLUSTER BY` for adaptive data layout. |
| `01b - Liquid Clustering (Original).py` | Python | Original liquid clustering walkthrough. |
| `01c - Liquid Clustering (Extended).py` | Python | Extended liquid clustering scenarios. |
| `02 - Zstandard Compression.py` | Python | Zstd compression codec for Delta tables. |
| `03 - Delta Lake vs Iceberg.py` | Python | Feature and behavior comparison between Delta Lake and Apache Iceberg. |
| `04 - Apache Iceberg V3 Features.py` | Python | Iceberg V3 capabilities and how they map to Delta. |
| `05 - UniForm.sql` | SQL | UniForm — expose Delta tables as Iceberg/Hudi for external readers. |
| `06 - Type Widening.py` | Python | Widen column types (e.g. `INT`→`BIGINT`) without rewriting data. |
| `07 - Vacuum.py` | Python | `VACUUM` to reclaim storage from stale files. |
| `08 - Deep Clone and Operations.sql` | SQL | Deep clone plus common table operations. |
| `09 - Collations.py` | Python | String collation support in Delta/SQL. |
| `10 - Delta Lake 3.0 Features.sql` | SQL | Overview of Delta Lake 3.0 features. |
| `11 - Variant Type.py` | Python | Variant type for semi-structured data. |
| `11b - Variant Type (JSON).py` | Python | Variant type working with JSON payloads. |
| `12 - Clustering.py` | Python | Clustering strategies for query performance. |
| `13 - Shallow Clone.sql` | SQL | Shallow clone for cheap, metadata-only copies. |

## Standalone notebooks

| File | Type | Description |
|------|------|-------------|
| `Delta Lake Column Mapping.ipynb` | Notebook | Column mapping for safe rename/drop and schema evolution via `delta.columnMapping.mode`. |
| `Delta Lake Deletion Vectors.py` | Notebook | Deletion vectors and their effect on MERGE performance (with/without, 20M-row demo). |
| `Delta Lake Identity column.ipynb` | Notebook | Identity columns (`GENERATED ALWAYS AS IDENTITY`) for surrogate keys. |
| `Delta Lake Variant.ipynb` | Notebook | Variant type for storing and querying JSON-like data with type preservation. |
