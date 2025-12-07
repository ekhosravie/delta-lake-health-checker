from pyspark.sql.functions import col
from pyspark.sql.types import *
import json

# =========================
# CONFIG: Catalog & Schema
# =========================
catalog_name = "workspace"
schema_name = "wms_silver"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# =========================
# Get table types from information_schema first
# =========================
table_types_df = spark.sql(f"""
    SELECT table_name, table_type 
    FROM information_schema.tables 
    WHERE table_catalog = '{catalog_name}' 
      AND table_schema = '{schema_name}'
""")

# Convert to dict for easy lookup
table_type_map = {}
for row in table_types_df.collect():
    table_type_map[row["table_name"]] = row["table_type"]

print(f"Found {len(table_type_map)} tables in {schema_name}")
for tbl, typ in table_type_map.items():
    print(f"  {tbl}: {typ}")

# =========================
# Get all tables in schema
# =========================
tables_df = spark.sql(f"SHOW TABLES IN {schema_name}") \
    .select(col("tableName"), col("isTemporary"))

results = []

for row in tables_df.collect():
    table_name = row["tableName"]
    is_temp = row["isTemporary"]
    full_table = f"{catalog_name}.{schema_name}.{table_name}"
    
    # Get the actual type from information_schema
    info_schema_type = table_type_map.get(table_name, "UNKNOWN")
    
    print(f"\n{'='*60}")
    print(f"Processing: {table_name} (info_schema type: {info_schema_type})")
    print(f"{'='*60}")

    # -------------------------
    # Table Type & Delta Detail
    # -------------------------
    table_type = info_schema_type  # Start with what information_schema says
    partition_cols_str = None
    liquid_cluster_cols = None
    table_size_gb = None
    num_files = None
    row_count = None
    
    # Only try DESCRIBE DETAIL for MANAGED or EXTERNAL tables
    if info_schema_type in ["MANAGED", "EXTERNAL"]:
        try:
            detail_df = spark.sql(f"DESCRIBE DETAIL {full_table}")
            
            if detail_df.count() > 0:
                detail = detail_df.collect()[0]
                format_type = detail["format"]
                
                print(f"✓ DESCRIBE DETAIL succeeded, format: {format_type}")
                
                if format_type == "delta":
                    # Get partition columns
                    partition_cols = detail["partitionColumns"]
                    partition_cols_str = ",".join(partition_cols) if partition_cols and len(partition_cols) > 0 else None
                    
                    # Get clustering columns
                    clustering_cols = detail["clusteringColumns"]
                    liquid_cluster_cols = ",".join(clustering_cols) if clustering_cols and len(clustering_cols) > 0 else None
                    
                    # Get size and files - keep in bytes for storage
                    size_bytes = detail["sizeInBytes"]
                    table_size_gb = float(size_bytes) if size_bytes else None
                    num_files = detail["numFiles"]
                    
                    # Get row count via SELECT COUNT(*)
                    try:
                        count_result = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]["cnt"]
                        row_count = count_result
                    except:
                        row_count = None
                    
                    # Display size in human-readable format
                    if size_bytes and size_bytes > 0:
                        if size_bytes < 1024:
                            size_display = f"{size_bytes} B"
                        elif size_bytes < 1024**2:
                            size_display = f"{round(size_bytes / 1024, 2)} KB"
                        elif size_bytes < 1024**3:
                            size_display = f"{round(size_bytes / (1024**2), 2)} MB"
                        else:
                            size_display = f"{round(size_bytes / (1024**3), 2)} GB"
                    else:
                        size_display = "0 B"
                    
                    print(f"  Partitions: {partition_cols_str or 'None'}")
                    print(f"  Clustering: {liquid_cluster_cols or 'None'}")
                    print(f"  Size: {size_display}, Files: {num_files}")
                else:
                    print(f"⚠ Format is {format_type}, not Delta")
            else:
                print(f"⚠ DESCRIBE DETAIL returned empty")
                
        except Exception as e:
            error_msg = str(e)
            print(f"✗ DESCRIBE DETAIL failed: {error_msg[:100]}")
    else:
        print(f"ℹ Skipping DESCRIBE DETAIL (type is {info_schema_type})")

    # -------------------------
    # Z-Order Columns (via DESCRIBE HISTORY) - Only for MANAGED/EXTERNAL
    # -------------------------
    zorder_cols = None
    last_optimize_ts = None
    
    if info_schema_type in ["MANAGED", "EXTERNAL"]:
        try:
            history_df = spark.sql(f"DESCRIBE HISTORY {full_table}")
            optimize_rows = history_df.filter(col("operation") == "OPTIMIZE") \
                                      .orderBy(col("timestamp").desc()) \
                                      .collect()
            if optimize_rows and "zOrderBy" in optimize_rows[0]["operationParameters"]:
                zorder_data = optimize_rows[0]["operationParameters"]["zOrderBy"]
                
                # Parse if it's a JSON string, otherwise use as-is
                if isinstance(zorder_data, str):
                    try:
                        zorder_list = json.loads(zorder_data)
                        zorder_cols = ",".join(zorder_list) if isinstance(zorder_list, list) else zorder_data
                    except:
                        zorder_cols = zorder_data
                else:
                    # If it's already a list
                    zorder_cols = ",".join(zorder_data) if isinstance(zorder_data, list) else str(zorder_data)
                
                print(f"  Z-Order: {zorder_cols}")
            
            if optimize_rows:
                last_optimize_ts = optimize_rows[0]["timestamp"]
        except:
            pass

    # -------------------------
    # Vacuum Information - Only for MANAGED/EXTERNAL
    # Note: VACUUM doesn't create history entries, so we check file timestamps
    # -------------------------
    last_vacuum_ts = None
    
    if info_schema_type in ["MANAGED", "EXTERNAL"]:
        try:
            # Get table file information to determine if vacuum was run
            # Files created/modified after a VACUUM should show recent timestamps
            files_df = spark.sql(f"SELECT modification_time FROM delta.`{spark.sql(f'DESCRIBE DETAIL {full_table}').select('location').collect()[0][0]}` WHERE is_dir = false LIMIT 1")
            
            if files_df.count() > 0:
                # Get the most recent file modification time
                file_mod_time = files_df.collect()[0][0]
                if file_mod_time:
                    last_vacuum_ts = file_mod_time
        except:
            # Fallback: Check if table has been modified recently
            try:
                history_df = spark.sql(f"DESCRIBE HISTORY {full_table}")
                if history_df.count() > 0:
                    # Get the timestamp of the most recent operation
                    latest_op = history_df.orderBy(col("timestamp").desc()).collect()[0]
                    # VACUUM doesn't show in history, but we can infer from operation timestamps
                    # For now, we'll leave it as None since VACUUM isn't tracked
                    pass
            except:
                pass

    # -------------------------
    # Bloom Index Columns - Only for MANAGED/EXTERNAL
    # -------------------------
    bloom_cols_str = None
    
    if info_schema_type in ["MANAGED", "EXTERNAL"]:
        try:
            import json
            history_df = spark.sql(f"DESCRIBE HISTORY {full_table}")
            bloom_rows = history_df.filter(col("operation") == "CREATE BLOOMFILTER INDEX").collect()
            
            if bloom_rows:
                bloom_cols = []
                for row in bloom_rows:
                    op_params = row['operationParameters']
                    if isinstance(op_params, dict) and 'columns' in op_params:
                        try:
                            columns_str = op_params['columns']
                            if isinstance(columns_str, str):
                                columns = json.loads(columns_str)
                            else:
                                columns = columns_str
                            
                            col_names = [c['name'] for c in columns if isinstance(c, dict) and 'name' in c]
                            bloom_cols.extend(col_names)
                        except:
                            pass
                
                bloom_cols_str = ",".join(list(set(bloom_cols))) if bloom_cols else None
                if bloom_cols_str:
                    print(f"  Bloom Filters: {bloom_cols_str}")
        except:
            pass

    # -------------------------
    # Optimization Score (0-4)
    # -------------------------
    optimization_score = sum([
        1 if partition_cols_str else 0,
        1 if liquid_cluster_cols else 0,
        1 if zorder_cols else 0,
        1 if bloom_cols_str else 0
    ])

    # -------------------------
    # Missing Optimization Flags
    # -------------------------
    partition_missing = partition_cols_str is None
    clustering_missing = liquid_cluster_cols is None
    zorder_missing = zorder_cols is None
    bloom_missing = bloom_cols_str is None

    # -------------------------
    # Append results
    # -------------------------
    results.append((
        table_name,
        table_type,
        partition_cols_str,
        liquid_cluster_cols,
        zorder_cols,
        bloom_cols_str,
        table_size_gb,
        num_files,
        row_count,
        last_optimize_ts,
        last_vacuum_ts,
        optimization_score,
        partition_missing,
        clustering_missing,
        zorder_missing,
        bloom_missing
    ))

# =========================
# Define schema explicitly
# =========================
schema = StructType([
    StructField("table_name", StringType()),
    StructField("table_type", StringType()),
    StructField("partition_columns", StringType()),
    StructField("liquid_cluster_columns", StringType()),
    StructField("zorder_columns", StringType()),
    StructField("bloom_index_columns", StringType()),
    StructField("table_size_bytes", LongType()),
    StructField("num_files", LongType()),
    StructField("row_count", LongType()),
    StructField("last_optimize_timestamp", TimestampType()),
    StructField("last_vacuum_timestamp", TimestampType()),
    StructField("optimization_score", IntegerType()),
    StructField("partition_missing", BooleanType()),
    StructField("clustering_missing", BooleanType()),
    StructField("zorder_missing", BooleanType()),
    StructField("bloom_missing", BooleanType())
])

final_df = spark.createDataFrame(results, schema)

# =========================
# Order by optimization_score + table_size
# =========================
final_df = final_df.orderBy(
    col("optimization_score").desc(),
    col("table_size_bytes").desc()
)

print(f"\n{'='*60}")
print("FINAL RESULTS")
print(f"{'='*60}\n")

display(final_df)
