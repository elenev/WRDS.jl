module WRDS

using LibPQ, Tables, PrettyTables

Base.@kwdef struct WRDSConnectionSettings
    dbname::String = "wrds"
    host::String = "wrds-pgdata.wharton.upenn.edu"
    port::Int = 9737
    username::String
    password::Union{String, Nothing} = nothing
    passfile::Union{String, Nothing} = nothing
end

function connect(; username, kwargs...)
    settings = WRDSConnectionSettings(; username, kwargs...)
    return connect(settings)
end

function connect(settings::WRDSConnectionSettings)
    connstr = "dbname=$(settings.dbname) host=$(settings.host) port=$(settings.port) user=$(settings.username)"
    if settings.password !== nothing
        connstr *= " password=$(settings.password)"
    elseif settings.passfile !== nothing
        connstr *= " passfile=$(settings.passfile)"
    end

    conn = LibPQ.Connection(connstr)
    return conn
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

function raw_sql(conn, query)
    res = execute(conn, query)
    data = columntable(res)
    return data
end

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