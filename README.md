# WRDS

This is a Julia package for accessing the Wharton Research Data Services (WRDS) data sets. It provides a simple interface to download data from WRDS using its PostgresSQL interface and load them into Julia. The package is inspired by the `wrds` python package and this tutorial by Yifan Liu on Julia Discourse: https://discourse.julialang.org/t/querying-wrds-data-using-julia/31835.

Note: You must have a valid WRDS account to use this package.

## Installation

```julia
pkg> add https://github.com/elenev/WRDS.jl
```

## Usage

### Connecting

There are two ways to use this package. The first opens a persistent connection to the WRDS server. You then pass this connection as an argument to functions that download data. When you're done, it is your responsibility to close the connection. This is important -- WRDS will temporarily lock your account if you have too many open connections.

To open a connection, call

```julia
using WRDS
wrds = WRDS.connect(username="myusername", password="mypassword")
```

The password argument is optional. If omitted, `connect()` will search for a `.pgpass` or `pgpass.conf` file in the default location (home directory on *nix, `%APPDATA%\postgresql` on Windows). If it finds one, it will use the password stored there. If it doesn't find one, it will prompt you for a password. You can also supply the path to the password file as the `passfile` argument. In general, it is a good idea to store your password in a password file rather than hardcoding it in your script.


In the second approach, you create a `WRDSConnectionSettings` object that contains your WRDS credentials. You pass this object as an argument to functions that download data. The function then opens a connection, downloads the data, and closes the connection in one step. If it encounters an error, it will close the connection before returning.

The `WRDSConnectionSettings` constructor follows the same syntax as `connect()`:

```julia
using WRDS
settings = WRDS.WRDSConnectionSettings(username="myusername", password="mypassword")
```

### Retrieving Data

All subsequent functions can take either a `WRDSConnectionSettings` object or a connection object as their first argument. 

#### List of Available Libraries

To get a list of available libraries, call

```julia
libraries = WRDS.get_libraries(wrds)
```

By default, this will return a vector of all libraries that you have access to through your subscription. It will also print a table of each parent SAS library, the child libraries as defined in the Postgres schema, and the URL of the child library's description on the WRDS website. The matching between parent an child libraries is imperfect. While every available library will be returned, some links may not work. The printout can be turned off by setting the `print` argument to `false`.

#### List of Available Tables

To get a list of available tables in a given library, call

```julia
tables = WRDS.get_tables(wrds, "crsp")
```

This will return a vector of all tables in the `crsp` library. It will also print a table of each table in the library and the URL of the table's description on the WRDS website. The printout can be turned off by setting the `print` argument to `false`.

Note: if the library is a parent SAS library, figuring out the URL takes an additional query to the WRDS server and some processing. If you want the printout but don't care about the accuracy of the URLs, you can speed things up by setting `verify_links` to `false`.

#### Downloading Data

There are two ways to download data from WRDS.

To download data from a table directly, call

```julia
data = WRDS.get_table(wrds, "crsp", "msf")
```

This will download the first 10 rows of the `msf` table in the `crsp` library. You can specify the number of rows to download with the `limit` argument. If you want to download the entire table, set `limit` to `nothing`. You can also pass a vector of `columns` to download only some columns, and `offset` to skip the first `offset` rows.

You can also download data from a table using a SQL query. The equivalent to the `get_table` call above would be:

```julia
data = WRDS.raw_sql(wrds, "crsp", "select * from msf limit 10")
```

In both cases, the data is returned as a `NamedTuple` of `LibPQ` columns. This output complies with the `Tables.jl` interface so it can be passed directly into a `DataFrame` constructor if you prefer to use `DataFrame`s.

```julia
using DataFrames
df = WRDS.get_table(wrds, "crsp", "msf") |> DataFrame
```

### Closing the Connection

If you opened a connection with `connect()`, you must close it when you're done. To close a connection, call

```julia
WRDS.close(wrds)
```

If you used the `WRDSConnectionSettings` approach, there are no connections to close. The connection is closed automatically when the function returns.