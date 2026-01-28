# WRDS.jl

*This package is not affiliated with WRDS or the University of Pennsylvania.*

A Julia package for accessing [Wharton Research Data Services (WRDS)](https://wrds-www.wharton.upenn.edu/) datasets via their PostgreSQL interface. Inspired by the Python `wrds` package and [this tutorial](https://discourse.julialang.org/t/querying-wrds-data-using-julia/31835) by Yifan Liu.

**Requirements:** A valid WRDS account with database access.

## Installation

```julia
pkg> add https://github.com/elenev/WRDS.jl
```

## Quick Start

```julia
using WRDS, DataFrames

# Connect to WRDS
conn = WRDS.connect(username="myusername")  # prompts for password

# List available libraries
libraries = WRDS.list_libraries(conn)

# List tables in a library
tables = WRDS.list_tables(conn, "crsp")

# Get table schema
cols = WRDS.describe_table(conn, "crsp", "msf")

# Download data
data = WRDS.get_table(conn, "crsp", "msf",
    columns=["permno", "date", "ret"],
    limit=1000) |> DataFrame

# Run custom SQL
data = WRDS.raw_sql(conn, "SELECT * FROM crsp.msf LIMIT 10") |> DataFrame

# Close connection when done
close(conn)
```

## Authentication

### Option 1: Password File (Recommended)

Store your credentials in a `.pgpass` file for secure, passwordless authentication:

**Location:** `~/.pgpass` (Linux/macOS) or `%APPDATA%\postgresql\pgpass.conf` (Windows)

**Format:**
```
wrds-pgdata.wharton.upenn.edu:9737:wrds:myusername:mypassword
```

Then connect without specifying credentials:
```julia
conn = WRDS.connect(username="myusername")  # reads password from .pgpass
```

Or let the package extract the username from the file:
```julia
settings = WRDS.WRDSConnectionSettings()  # uses ~/.pgpass for everything
conn = WRDS.connect(settings)
```

### Option 2: Direct Password

```julia
conn = WRDS.connect(username="myusername", password="mypassword")
```

### Option 3: Interactive Prompt

```julia
conn = WRDS.connect(username="myusername")  # prompts if no .pgpass found
```

## Connection Management

### Persistent Connection

Open a connection, use it for multiple queries, then close it:

```julia
conn = WRDS.connect(username="myusername", password="mypassword")

# Multiple queries...
libs = WRDS.list_libraries(conn)
data = WRDS.get_table(conn, "crsp", "msf", limit=100)

# Important: close when done to avoid WRDS account lockout
close(conn)
```

### Automatic Connection Management

Pass a `WRDSConnectionSettings` object instead of a connection. The connection opens, runs the query, and closes automatically:

```julia
settings = WRDS.WRDSConnectionSettings(username="myusername", password="mypassword")

# Each call handles its own connection
libs = WRDS.list_libraries(settings)
data = WRDS.get_table(settings, "crsp", "msf", limit=100)
# No close() needed
```

## API Reference

### Exploring Data

#### `list_libraries`

List available WRDS libraries/schemas:

```julia
libraries = WRDS.list_libraries(conn)
libraries = WRDS.list_libraries(conn; print=false)      # suppress table output
libraries = WRDS.list_libraries(conn; sasonly=true)     # SAS libraries only
```

#### `list_tables`

List tables in a library:

```julia
tables = WRDS.list_tables(conn, "crsp")
tables = WRDS.list_tables(conn, "crsp"; print=false)         # suppress output
tables = WRDS.list_tables(conn, "crsp"; verify_links=false)  # faster, less accurate URLs
```

#### `describe_table`

Get column information for a table:

```julia
columns = WRDS.describe_table(conn, "crsp", "msf")
columns = WRDS.describe_table(conn, "crsp", "msf"; print=false)
columns = WRDS.describe_table(conn, "crsp", "msf";
    properties=["is_nullable", "data_type", "character_maximum_length"])
```

### Retrieving Data

#### `get_table`

Download data from a table with optional filtering:

```julia
# Basic usage (first 10 rows)
data = WRDS.get_table(conn, "crsp", "msf")

# Select columns and limit rows
data = WRDS.get_table(conn, "crsp", "msf",
    columns=["permno", "date", "ret"],
    limit=1000)

# With WHERE clause
data = WRDS.get_table(conn, "crsp", "msf",
    columns=["permno", "date", "ret"],
    where="permno = 10000",
    limit=1000)

# Multiple conditions (joined with AND)
data = WRDS.get_table(conn, "crsp", "msf",
    where=["permno = 10000", "date >= '2020-01-01'"],
    limit=1000)

# Pagination
data = WRDS.get_table(conn, "crsp", "msf", limit=100, offset=500)

# Full table (use with caution on large tables!)
data = WRDS.get_table(conn, "crsp", "msf", limit=nothing)
```

#### `raw_sql`

Execute custom SQL queries:

```julia
data = WRDS.raw_sql(conn, """
    SELECT a.permno, a.date, a.ret, b.shrout
    FROM crsp.msf a
    JOIN crsp.msenames b ON a.permno = b.permno
    WHERE a.date >= '2020-01-01'
    LIMIT 1000
""")
```

### Working with Results

All data retrieval functions return a `NamedTuple` compatible with [Tables.jl](https://github.com/JuliaData/Tables.jl):

```julia
using DataFrames

# Convert to DataFrame
df = WRDS.get_table(conn, "crsp", "msf", limit=100) |> DataFrame

# Or use the constructor directly
data = WRDS.raw_sql(conn, "SELECT * FROM crsp.msf LIMIT 100")
df = DataFrame(data)
```

## Troubleshooting

### Connection Lockout

WRDS limits concurrent connections. If you're locked out:
1. Wait a few minutes for connections to timeout
2. Always `close(conn)` when done
3. Consider using `WRDSConnectionSettings` for automatic management

### Library vs Schema Names

WRDS uses both SAS library names (e.g., `crsp`) and PostgreSQL schema names (e.g., `crsp_a_stock`). The `list_libraries` function shows both and their mappings.

## Contributing

Issues and pull requests are welcome at [https://github.com/elenev/WRDS.jl](https://github.com/elenev/WRDS.jl). I'm not registering the package yet because I can't commit to actively maintain it. If that's something you want to do, let me know.
