using WRDS
using Test

# Helper: create temp pgpass file for testing
# Note: f comes first to support do-block syntax
function with_temp_pgpass(f::Function, content::String)
    path = tempname()
    try
        open(path, "w") do io
            Base.write(io, content)
        end
        f(path)
    finally
        isfile(path) && rm(path)
    end
end

@testset "WRDS.jl" begin

    @testset "Credential Types" begin
        @testset "WRDSPasswordCredentials" begin
            cred = WRDS.WRDSPasswordCredentials("testuser", "testpass")
            @test cred.username == "testuser"
            @test cred.password isa Base.SecretBuffer
        end
    end

    @testset "Connection Settings" begin
        @testset "with username and password" begin
            settings = WRDS.WRDSConnectionSettings(username="testuser", password="testpass")
            @test settings.credentials.username == "testuser"
            @test settings.dbname == WRDS.DEFAULT_DBNAME
            @test settings.host == WRDS.DEFAULT_HOST
            @test settings.port == WRDS.DEFAULT_PORT
            @test settings.credentials isa WRDS.WRDSPasswordCredentials
        end

        @testset "with username and passfile" begin
            with_temp_pgpass("*:*:*:testuser:pass\n") do path
                settings = WRDS.WRDSConnectionSettings(username="testuser", passfile=path)
                @test settings.credentials isa WRDS.WRDSPassfileCredentials
                @test settings.credentials.username == "testuser"
                @test settings.credentials.passfile == path
            end
        end

        @testset "with credentials object" begin
            cred = WRDS.WRDSPasswordCredentials("creduser", "credpass")
            settings = WRDS.WRDSConnectionSettings(credentials=cred)
            @test settings.credentials === cred
        end

        @testset "error on conflicting args" begin
            cred = WRDS.WRDSPasswordCredentials("user1", "pass1")
            @test_throws ErrorException WRDS.WRDSConnectionSettings(
                username="user2", credentials=cred
            )
        end

        @testset "custom host/port/dbname" begin
            settings = WRDS.WRDSConnectionSettings(
                username="testuser", password="testpass",
                host="custom.host.com", port=5432, dbname="customdb"
            )
            @test settings.host == "custom.host.com"
            @test settings.port == 5432
            @test settings.dbname == "customdb"
        end
    end

    @testset "pgpass Parsing" begin
        @testset "exact match" begin
            content = "wrds-pgdata.wharton.upenn.edu:9737:wrds:myuser:mypass\n"
            with_temp_pgpass(content) do path
                user = WRDS._get_pg_username(
                    WRDS.DEFAULT_HOST, WRDS.DEFAULT_PORT, WRDS.DEFAULT_DBNAME;
                    pgpass_path=path
                )
                @test user == "myuser"
            end
        end

        @testset "wildcard match" begin
            content = "*:*:*:wildcarduser:pass\n"
            with_temp_pgpass(content) do path
                user = WRDS._get_pg_username(
                    WRDS.DEFAULT_HOST, WRDS.DEFAULT_PORT, WRDS.DEFAULT_DBNAME;
                    pgpass_path=path
                )
                @test user == "wildcarduser"
            end
        end

        @testset "partial wildcard" begin
            content = "*:9737:wrds:partialuser:pass\n"
            with_temp_pgpass(content) do path
                user = WRDS._get_pg_username(
                    WRDS.DEFAULT_HOST, WRDS.DEFAULT_PORT, WRDS.DEFAULT_DBNAME;
                    pgpass_path=path
                )
                @test user == "partialuser"
            end
        end

        @testset "first match wins" begin
            content = """
            wrds-pgdata.wharton.upenn.edu:9737:wrds:firstuser:pass
            wrds-pgdata.wharton.upenn.edu:9737:wrds:seconduser:pass
            """
            with_temp_pgpass(content) do path
                user = WRDS._get_pg_username(
                    WRDS.DEFAULT_HOST, WRDS.DEFAULT_PORT, WRDS.DEFAULT_DBNAME;
                    pgpass_path=path
                )
                @test user == "firstuser"
            end
        end

        @testset "skips comments" begin
            content = "# This is a comment\nwrds-pgdata.wharton.upenn.edu:9737:wrds:realuser:pass\n"
            with_temp_pgpass(content) do path
                user = WRDS._get_pg_username(
                    WRDS.DEFAULT_HOST, WRDS.DEFAULT_PORT, WRDS.DEFAULT_DBNAME;
                    pgpass_path=path
                )
                @test user == "realuser"
            end
        end

        @testset "no match returns nothing" begin
            content = "otherhost:5432:otherdb:otheruser:pass\n"
            with_temp_pgpass(content) do path
                user = WRDS._get_pg_username(
                    WRDS.DEFAULT_HOST, WRDS.DEFAULT_PORT, WRDS.DEFAULT_DBNAME;
                    pgpass_path=path
                )
                @test user === nothing
            end
        end

        @testset "missing file throws error" begin
            @test_throws ErrorException WRDS._get_pg_username(
                WRDS.DEFAULT_HOST, WRDS.DEFAULT_PORT, WRDS.DEFAULT_DBNAME;
                pgpass_path="/nonexistent/path/.pgpass"
            )
        end

        @testset "handles port as integer" begin
            content = "wrds-pgdata.wharton.upenn.edu:9737:wrds:intportuser:pass\n"
            with_temp_pgpass(content) do path
                user = WRDS._get_pg_username(
                    WRDS.DEFAULT_HOST, 9737, WRDS.DEFAULT_DBNAME;
                    pgpass_path=path
                )
                @test user == "intportuser"
            end
        end
    end

    @testset "Helper Functions" begin
        @testset "stringify_where" begin
            @test WRDS.stringify_where("'value'") == " = 'value'"
            @test WRDS.stringify_where(["'a'", "'b'"]) == " IN ('a', 'b')"
            @test WRDS.stringify_where(["single"]) == " IN (single)"
        end

        @testset "_credential_string for password" begin
            cred = WRDS.WRDSPasswordCredentials("myuser", "mypass")
            str = WRDS._credential_string(cred)
            @test contains(str, "user=myuser")
            @test contains(str, "password=mypass")
            @test contains(str, "sslmode=require")
        end

        @testset "_credential_string for passfile" begin
            with_temp_pgpass("*:*:*:fileuser:pass\n") do path
                cred = WRDS.WRDSPassfileCredentials("fileuser", path)
                str = WRDS._credential_string(cred)
                @test contains(str, "user=fileuser")
                @test contains(str, "passfile=$path")
            end
        end

        @testset "rearrange_libraries" begin
            # Mock library data structure matching what the DB returns
            lib = (
                schemaname = ["crsp", "crsp_a_stock", "crsp_q_stock", "comp", "comp_bank"],
                relkind_a = ["{v}", "{r,f}", "{r,f}", "{v}", "{r,f}"]
            )
            views, mapping = WRDS.rearrange_libraries(lib)
            @test "crsp" in views
            @test "comp" in views
            @test length(views) == 2
            # Check mapping contains expected pairs
            @test any(p -> p.first == "crsp" && p.second == "crsp_a_stock", mapping)
        end
    end

    # Integration tests - only run when credentials are available
    credentials_available = haskey(ENV, "WRDS_USERNAME") || isfile(expanduser("~/.pgpass"))

    if credentials_available
        @testset "Integration Tests" begin
            # Get credentials from env or default pgpass
            username = get(ENV, "WRDS_USERNAME", nothing)
            password = get(ENV, "WRDS_PASSWORD", nothing)

            conn = if username !== nothing && password !== nothing
                WRDS.connect(username=username, password=password)
            elseif username !== nothing
                WRDS.connect(username=username)
            else
                # Use default pgpass
                settings = WRDS.WRDSConnectionSettings()
                WRDS.connect(settings)
            end

            @testset "list_libraries" begin
                libs = WRDS.list_libraries(conn; print=false, sasonly=true)
                @test libs isa Vector
                @test length(libs) > 0
                # Check for common WRDS libraries
                @test any(lib -> occursin("crsp", lowercase(lib)), libs)
            end

            @testset "list_tables" begin
                tables = WRDS.list_tables(conn, "crsp"; print=false)
                @test tables isa AbstractVector
                @test length(tables) > 0
            end

            @testset "describe_table" begin
                # Use the underlying postgres schema for describe
                cols = WRDS.describe_table(conn, "crsp_a_stock", "mse"; print=false)
                @test cols isa AbstractVector
                @test length(cols) > 0
            end

            @testset "raw_sql" begin
                result = WRDS.raw_sql(conn, "SELECT 1 AS test_col")
                @test haskey(result, :test_col)
                @test result.test_col[1] == 1
            end

            @testset "get_table" begin
                @testset "basic query" begin
                    data = WRDS.get_table(conn, "crsp_a_stock", "mse"; limit=5)
                    @test length(first(values(data))) == 5
                end

                @testset "with columns" begin
                    data = WRDS.get_table(conn, "crsp_a_stock", "mse";
                        columns=["permno", "date"], limit=5)
                    @test haskey(data, :permno)
                    @test haskey(data, :date)
                    @test length(keys(data)) == 2
                end

                @testset "with where clause" begin
                    data = WRDS.get_table(conn, "crsp_a_stock", "mse";
                        columns=["permno"],
                        where="permno = 10000",
                        limit=10)
                    @test all(x -> x == 10000, data.permno)
                end

                @testset "with offset" begin
                    # Just verify offset doesn't error and returns data
                    data = WRDS.get_table(conn, "crsp_a_stock", "mse";
                        columns=["permno"], limit=5, offset=10)
                    @test length(data.permno) == 5
                end
            end

            # Clean up
            close(conn)
        end
    else
        @info "Skipping integration tests: No WRDS credentials found. " *
              "Set WRDS_USERNAME/WRDS_PASSWORD env vars or configure ~/.pgpass"
    end

end
