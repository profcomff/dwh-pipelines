INSERT INTO "DM_MONITORING".db_monitoring_snp 
(id, table_name, table_schema, table_size_mb, indexes_size_mb, total_size_mb, state_dt)
SELECT
    to_char(CURRENT_DATE, 'YYYYMMDD')::integer,
    table_name,
    table_schema,
    table_size/(1024*1024),
    indexes_size/(1024*1024),
    total_size/(1024*1024),
    '{{ ds }}'::Date
FROM (
    SELECT
        table_name,
        table_schema,
        pg_table_size(table_name) AS table_size,
        pg_indexes_size(table_name) AS indexes_size,
        pg_total_relation_size(table_name) AS total_size
    FROM (
        SELECT ('"' || table_schema || '"."' || table_name || '"') AS table_name,
        table_schema
        FROM information_schema.tables
    ) AS all_tables
    ORDER BY total_size DESC
) AS pretty_sizes
LIMIT 100000; -- чтобы не раздуло
