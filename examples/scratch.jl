using Pkg
Pkg.activate("./examples")

using WRDS
using DataFrames

# Connect to WRDS
wrds = WRDS.connect(username="velenev")

# Get list of libraries/datasets
libraries = WRDS.list_libraries(wrds)

# Get list of tables in a library
tables = WRDS.list_tables(wrds, "crsp")

# Describe a table
cols = WRDS.describe_table(wrds, "crsp", "mse")

# Run a raw SQL query
sql = "select permno, permco, date from crsp_a_stock.mse where ticker = 'MSFT'"
df = WRDS.raw_sql(wrds, sql) |> DataFrame

# Retrieve a table
df = WRDS.get_table(wrds, "crsp", "mse") |> DataFrame

# Close connection
WRDS.close(wrds)


# Random stuff
query = raw"""
WITH pgobjs AS (
    -- objects we care about - tables, views, foreign tables, partitioned tables
    SELECT oid, relnamespace, relkind
    FROM pg_class
    WHERE relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char", 'p'::"char"])
),
schemas AS (
    -- schemas we have usage on that represent products
    SELECT nspname AS schemaname,
        pg_namespace.oid,
        array_agg(DISTINCT relkind) AS relkind_a
    FROM pg_namespace
    JOIN pgobjs ON pg_namespace.oid = relnamespace
    WHERE nspname !~ '(^pg_)|(_old$)|(_new$)|(information_schema)'
        AND has_schema_privilege(nspname, 'USAGE') = TRUE
    GROUP BY nspname, pg_namespace.oid
)
SELECT schemaname, relkind_a, 1
FROM schemas
WHERE relkind_a != ARRAY['v'::"char"] -- any schema except only views
UNION
-- schemas w/ views (aka "friendly names") that reference accessable product tables
SELECT nv.schemaname, relkind_a, 2
FROM schemas nv
"""
df = WRDS.raw_sql(settings, query) |> DataFrame