module WRDS

using LibPQ, Tables, PrettyTables

# Defaults
const DEFAULT_DBNAME = "wrds"
const DEFAULT_HOST = "wrds-pgdata.wharton.upenn.edu"
const DEFAULT_PORT = 9737
const DEFAULT_PASSFILE = expanduser("~/.pgpass")    

abstract type WRDSCredentials end
struct WRDSPasswordCredentials <: WRDSCredentials
    username::String
    password::Base.SecretBuffer
end
function WRDSPasswordCredentials(username::String, password::String)
    secret = Base.SecretBuffer(password)
    return WRDSPasswordCredentials(username, secret)
end
function WRDSPasswordCredentials(username::String)
    secret = Base.getpass("Enter your WRDS password for user '$username'")
    return WRDSPasswordCredentials(username, secret)
end

struct WRDSPassfileCredentials <: WRDSCredentials
    username::String
    passfile::String
end
function WRDSPassfileCredentials(passfile::String)
    username = _get_pg_username(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DBNAME; pgpass_path=passfile)
    if username === nothing
        error("No matching entry found in pgpass file at $passfile for host=$(DEFAULT_HOST), port=$(DEFAULT_PORT), dbname=$(DEFAULT_DBNAME)")
    end
    return WRDSPassfileCredentials(username, passfile)
end

WRDSPassfileCredentials() = WRDSPassfileCredentials(DEFAULT_PASSFILE)


function _get_pg_username(host::String, port::Union{String,Int}, dbname::String; pgpass_path::String=DEFAULT_PASSFILE)
    
    # 1. Open the file safely
    if !isfile(pgpass_path)
        error("pgpass file not found at $pgpass_path")
    end

    open(pgpass_path, "r") do io
        for line in eachline(io)
            # 2. Skip comments
            startswith(line, "#") && continue
            
            # 3. Parse fields (handles escaped colons if necessary, but keep it simple for now)
            fields = split(strip(line), ":")
            
            # Standard .pgpass has 5 fields
            if length(fields) >= 4
                f_host, f_port, f_db, f_user = fields[1:4]
                #println("Checking pgpass entry: host=$f_host vs. $host, port=$f_port vs. $port, db=$f_db vs. $dbname")

                # 4. Match Logic: exact match or wildcard '*'
                host_match = (f_host == host) || (f_host == "*")
                port_match = (f_port == string(port)) || (f_port == "*")
                db_match   = (f_db   == dbname) || (f_db   == "*")

                #println("Match results: host_match=$host_match, port_match=$port_match, db_match=$db_match")
                
                if host_match && port_match && db_match
                    return f_user
                end
            end
        end
        return nothing # No match found
    end
end

"""
    struct WRDSConnectionSettings

A struct representing the connection settings for accessing WRDS (Wharton Research Data Services) database.
    
# Fields
- dbname::String: The name of the database. Default is "wrds".
- host::String: The hostname or IP address of the WRDS database server. Default is "wrds-pgdata.wharton.upenn.edu".
- port::Int: The port number of the WRDS database server. Default is 9737.
- username::String: The username for authenticating with the WRDS database.
- password::Union{String, Nothing}: The password for authenticating with the WRDS database. Default is `nothing`.
- passfile::Union{String, Nothing}: The path to a `.pgpass` file containing the password for authenticating with the WRDS database. Default is `nothing`.
    
# Example
```julia
settings = WRDSConnectionSettings(username="myusername", password="mypassword")
```
"""
struct WRDSConnectionSettings{C<:WRDSCredentials}
    dbname::String
    host::String
    port::Int
    credentials::C 
end
function WRDSConnectionSettings(; kwargs...)
    if haskey(kwargs, :username)
        if haskey(kwargs, :credentials)
            error("Cannot provide both 'credentials' and 'username' argument.")
        end
        username = kwargs[:username]

        if haskey(kwargs, :password)
            password = kwargs[:password]
            cred = WRDSPasswordCredentials(username, password)
        elseif haskey(kwargs, :passfile)
            passfile = kwargs[:passfile]
            cred = WRDSPassfileCredentials(username, passfile)
        else
            cred = WRDSPasswordCredentials(username)
        end
    elseif haskey(kwargs, :credentials)
        cred = kwargs[:credentials]
    else
        cred = WRDSPassfileCredentials()
    end
    dbname = get(kwargs, :dbname, DEFAULT_DBNAME)
    host = get(kwargs, :host, DEFAULT_HOST)
    port = get(kwargs, :port, DEFAULT_PORT)
    return WRDSConnectionSettings{typeof(cred)}(dbname, host, port, cred)
end


"""
    connect(; username, kwargs...)

Create a `WRDSConnectionSettings` object with the provided `username` and any additional keyword arguments, and then establish a connection to the WRDS database.

# Arguments
- `username::String`: The username for authenticating with the WRDS database.
- `kwargs...`: Additional keyword arguments to be passed to the `WRDSConnectionSettings` constructor.

# Returns
- `conn::LibPQ.Connection`: A connection to the WRDS database.
"""
function connect(; username, kwargs...)
    settings = WRDSConnectionSettings(; username, kwargs...)
    return connect(settings)
end

"""
    connect(settings::WRDSConnectionSettings)

Establish a connection to the WRDS database using the provided `settings`.

# Arguments
- `settings::WRDSConnectionSettings`: The settings to use for the connection.

# Returns
- `conn::LibPQ.Connection`: A connection to the WRDS database.
"""
function connect(settings::WRDSConnectionSettings)
    connstr = "dbname=$(settings.dbname) host=$(settings.host) port=$(settings.port) " *
                _credential_string(settings.credentials)

    conn = LibPQ.Connection(connstr)
    return conn
end

function _credential_string(creds::WRDSPasswordCredentials)
    seekstart(creds.password)
    return "user=$(creds.username) password=$(read(creds.password, String)) sslmode=require"
end

function _credential_string(creds::WRDSPassfileCredentials)
    return "user=$(creds.username) passfile=$(creds.passfile)"
end


function wrap_function(f::Function, settings::WRDSConnectionSettings, args...; kwargs...)
    conn = connect(settings)
    try
        return f(conn, args...; kwargs...)
    finally
        close(conn)
    end
end

list_libraries(settings::WRDSConnectionSettings; kwargs...) = wrap_function(list_libraries, settings)
list_tables(settings::WRDSConnectionSettings, library; kwargs...) = wrap_function(list_tables, settings, library; kwargs...)
describe_table(settings::WRDSConnectionSettings, library, table; kwargs...) = wrap_function(describe_table, settings, library, table; kwargs...)
raw_sql(settings::WRDSConnectionSettings, query) = wrap_function(raw_sql, settings, query)
get_table(settings::WRDSConnectionSettings, library, table; kwargs...) = wrap_function(get_table, settings, library, table; kwargs...)

"""
    list_libraries(conn, print=true, sasonly=false)

Retrieve and optionally print the schema of the library from the WRDS database connection.

# Arguments
- `conn`: The connection to the WRDS database, or a `WRDSConnectionSettings`` object.
- `print::Bool`: If true, the function will print a pretty version of the libraries.
- `sasonly::Bool`: If true, the function will only return SAS libraries (parent libraries).

# Returns
- libraries::Vector{String} library names
"""
function list_libraries(conn::LibPQ.Connection; print=true, sasonly=false)

    data = get_library_schema(conn)

    if print || sasonly
        views, mapping = rearrange_libraries(data)
        if print
            pretty_libraries(mapping)
        end
        return views
    else
        return data[1]

    end
end

function get_library_schema(conn::LibPQ.Connection)
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
    SELECT schemaname, relkind_a
    FROM schemas
    WHERE relkind_a != ARRAY['v'::"char"] -- any schema except only views
    UNION
    -- schemas w/ views (aka "friendly names") that reference accessable product tables
    SELECT nv.schemaname, nv.relkind_a
    FROM schemas nv
    JOIN pgobjs v ON nv.oid = v.relnamespace AND v.relkind = 'v'::"char"
    JOIN pg_depend dv ON v.oid = dv.refobjid AND dv.refclassid = 'pg_class'::regclass::oid
        AND dv.classid = 'pg_rewrite'::regclass::oid AND dv.deptype = 'i'::"char"
    JOIN pg_depend dt ON dv.objid = dt.objid AND dv.refobjid <> dt.refobjid
        AND dt.classid = 'pg_rewrite'::regclass::oid
        AND dt.refclassid = 'pg_class'::regclass::oid
    JOIN pgobjs t ON dt.refobjid = t.oid
        AND (t.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char", 'p'::"char"]))
    JOIN schemas nt ON t.relnamespace = nt.oid
    GROUP BY nv.schemaname, nv.relkind_a
    ORDER BY 1;
            """

    res = execute(conn, query)
    return columntable(res)
end

function rearrange_libraries(lib)
    mapping = Vector{Pair}()
    views = Vector{String}()
    parent_view = missing
    n_children = -1
    for (name,rel) in zip(lib.schemaname, lib.relkind_a)
        if contains(rel,"v")
            if n_children == 0
                push!(mapping, parent_view => missing)
            end
            parent_view = name
            push!(views, name)
            n_children = 0
        else
            if startswith(name, parent_view)
                push!(mapping, parent_view => name)
                n_children += 1
            end
        end
    end
    return views, mapping
end

function pretty_libraries(mapping)
    N = length(mapping)
    prefix = "https://wrds-www.wharton.upenn.edu/data-dictionary/"
    mapping_matrix = hcat(first.(mapping), last.(mapping), repeat([prefix], N))
    mapping_matrix[:,end] .*= mapping_matrix[:,2]
    mapping_matrix[:,end] .*= "/"
    pretty_table(mapping_matrix, crop=:none, header=["SAS Library", "Postgres Schema", "URL"], alignment=:l) 
end

function stringify_where(where)
    if where isa String
        return " = " * where
    else
        return " IN (" * join(where, ", ") * ")"
    end
end

"""
    list_tables(conn, library, print=false, verify_links=false)

Retrieve and optionally print the list of tables in a given library from the WRDS database connection. Optionally verify the links in each table.

# Arguments
- `conn`: The connection to the WRDS database, or a `WRDSConnectionSettings`` object.
- `library`: The library in the WRDS database to list tables from.
- `print::Bool`: If true, the function will print a pretty version of the tables.
- `verify_links::Bool`: If true, the function will verify the links in each table. This is only relevant for SAS libraries, which do not have pages in the web data dictionary. 
   With `verify_links=true`, the function will attempt to find the corresponding Postgres schema for the SAS library and construct the URL for each table.

# Returns
- A list of tables in the specified library.
"""
function list_tables(conn::LibPQ.Connection, library; print=true, verify_links=true)
    query = """
    SELECT  distinct table_name
    FROM information_schema.columns
    WHERE table_schema = '$(library)'
    ORDER BY table_name
            """

    res = execute(conn, query)
    data = columntable(res)[1]
    res = nothing

    if print
        prefix = "https://wrds-www.wharton.upenn.edu/data-dictionary/"
        if verify_links
            # Links won't work by default if the library is a SAS library, not a Postgres schema
            libs = get_library_schema(conn)
            views, mapping = rearrange_libraries(libs)
            if library in views
                possible_libraries = [v for (k,v) in mapping if k == library && !ismissing(v)]
                if length(possible_libraries) > 0
                    where_str = "(" * join("'" .* possible_libraries .* "'", ", ") * ")"
                    lookup_query = """
                        SELECT distinct table_schema, table_name
                        FROM information_schema.columns
                        WHERE table_schema IN $(where_str)
                        ORDER BY table_name
                        """              
                    res = execute(conn, lookup_query)     
                    lookup_data = columntable(res)
                    newdata = Vector{String}()
                    urls = Vector{String}()
                    for (lib,table) in zip(lookup_data...)
                        if table in data
                            push!(newdata, table)
                            push!(urls, join([prefix, lib, "/", table], ""))
                        end
                    end
                    data = newdata
                else
                    urls = repeat([missing], length(data))
                end
            else
                urls = [join([prefix, library, "/", table], "") for table in data]
            end
        else
            urls = [join([prefix, library, "/", table], "") for table in data]
        end
        pretty_table((; data,urls), crop=:none, header=["Table", "URL"], alignment=:l)
    end
    return data
end

"""
    describe_table(conn, library, table, print=true, properties=["is_nullable","data_type"])

Retrieve and print the description of a specific table from a given library in the WRDS database connection.

# Arguments
- `conn`: The connection to the WRDS database.
- `library`: The library in the WRDS database where the table is located.
- `table`: The table in the library to describe.
- `print::Bool`: If true (default), the function will print a pretty version of the table description: approximate number of rows, column names and some additional column properties.
- properties::Vector{String}: A list of additional column properties to include in the description. Default is `["is_nullable","data_type"]`.

# Returns
- A vector of column names
"""
function describe_table(conn, library, table; 
                        print=true,
                        properties=["is_nullable","data_type"])
    pushfirst!(properties, "column_name")
    colstring = join(properties, ", ")
    query = """
    SELECT $(colstring)
    FROM information_schema.columns
    WHERE table_schema = '$(library)'
    AND table_name = '$(table)'
    ORDER BY ordinal_position
            """

    res = execute(conn, query)
    data = columntable(res)

    if print
        nrows = get_row_count(conn, library, table)
        println("Table: $(library).$(table) (~$nrows rows)")
        pretty_table(data, crop=:none)
    end

    return data[1]
end

function get_row_count(conn::LibPQ.Connection, library, table)
    query = """
    EXPLAIN (FORMAT 'json')  SELECT 1 
    FROM $(library).$(table)
            """

    res = execute(conn, query)
    data = columntable(res)[1][1]
    m = match(Regex("\\\"Plan Rows\\\": (\\d+),\\n"), data)
    return parse(Int, m.captures[1])
end

"""
    raw_sql(conn, query)

Run a raw SQL query on the WRDS database connection.

# Arguments
- conn::Union{LibPQ.Connection, WRDSConnectionSettings}: A LibPQ connection object representing the connection to the PostgreSQL database, or the WRDS connection settings.
- query::String: The SQL query to execute.

# Returns
- data::NamedTuple: A Tables.jl compatible NamedTuple containing the retrieved data.
"""
function raw_sql(conn, query)
    res = execute(conn, query)
    data = columntable(res)
    return data
end

"""
    get_table(conn, library, table; 
          columns=nothing, 
          where=nothing, 
          limit=10,
          offset=0)

Retrieve data from a table in a PostgreSQL database using a LibPQ connection.

# Arguments
- conn::Union{LibPQ.Connection, WRDSConnectionSettings}: A LibPQ connection object representing the connection to the PostgreSQL database, or the WRDS connection settings.
- library::String: The name of the database schema where the table is located.
- table::String: The name of the table from which to retrieve data.
- columns::Vector{String}|Nothing: (Optional) A vector of column names to select. Default is `nothing`, which selects all columns.
- where::Union{String, Vector{String}, Nothing}: (Optional) A string or vector of strings representing the conditions to filter the rows. Default is `nothing`, which means no filtering.
- limit::Int: (Optional) The maximum number of rows to retrieve. Default is 10.
- offset::Int: (Optional) The number of rows to skip before starting to return rows. Default is 0.

# Returns
- data::DataFrame: A DataFrame containing the retrieved data.

# Example
```julia
conn = WRDSConnectionSettings(username="myusername", password="mypassword")
data = get_table(conn, "crsp", "msf", columns=["permno", "ret"], limit=100, offset=50)
```
"""
function get_table(conn::LibPQ.Connection, library, table; 
                   columns=nothing, 
                   where=nothing, 
                   limit=10,
                   offset=0)
    colstring = columns === nothing ? "*" : join(columns, ", ")
    query = """
    SELECT $colstring
    FROM $(library).$(table)
    """

    if where !== nothing
        if !(where isa String)
            where = join(where, " AND ")
        end
        query *= " WHERE $where"
    end

    if limit !== nothing
        query *= " LIMIT $limit"
    end

    if offset !== nothing
        query *= " OFFSET $offset"
    end

    res = execute(conn, query)
    data = columntable(res)
    return data
end

end