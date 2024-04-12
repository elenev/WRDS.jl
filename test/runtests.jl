using WRDS
using Test

@testset "WRDS.jl" begin
    # Write your tests here.
end

wrds = WRDS.connect(username="velenev")
out = WRDS.raw_sql(wrds, 
    "select permno, permco, date from crsp_a_stock.mse where ticker = 'MSFT'")