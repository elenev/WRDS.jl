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
