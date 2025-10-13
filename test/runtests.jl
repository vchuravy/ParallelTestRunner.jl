using ParallelTestRunner
using Test

cd(@__DIR__)

@testset "ParallelTestRunner" verbose=true begin

@testset "basic test" begin
    io = IOBuffer()
    runtests(["--verbose"]; stdout=io, stderr=io)
    str = String(take!(io))

    println()
    println("Showing the output of one test run:")
    println("-"^80)
    print(str)
    println("-"^80)
    println()

    @test contains(str, r"basic .+ started at")
    @test contains(str, "SUCCESS")
end

@testset "custom tests and init code" begin
    init_code = quote
        using Test
        should_be_defined() = true

        macro should_also_be_defined()
            return :(true)
        end
    end
    custom_tests = Dict(
        "custom" => quote
            @test should_be_defined()
            @test @should_also_be_defined()
        end
    )

    io = IOBuffer()
    runtests(["--verbose"]; init_code, custom_tests, stdout=io, stderr=io)

    str = String(take!(io))
    @test contains(str, r"basic .+ started at")
    @test contains(str, r"custom .+ started at")
    @test contains(str, "SUCCESS")
end

@testset "custom worker" begin
    function test_worker(name)
        if name == "needs env var"
            return addworker(env = ["SPECIAL_ENV_VAR" => "42"])
        end
        return nothing
    end
    custom_tests = Dict(
        "needs env var" => quote
            @test ENV["SPECIAL_ENV_VAR"] == "42"
        end,
        "doesn't need env var" => quote
            @test !haskey(ENV, "SPECIAL_ENV_VAR")
        end
    )

    io = IOBuffer()
    runtests(["--verbose"]; test_worker, custom_tests, stdout=io, stderr=io)

    str = String(take!(io))
    @test contains(str, r"basic .+ started at")
    @test contains(str, r"needs env var .+ started at")
    @test contains(str, r"doesn't need env var .+ started at")
    @test contains(str, "SUCCESS")
end

@testset "failing test" begin
    custom_tests = Dict(
        "failing test" => quote
            @test 1 == 2
        end
    )

    io = IOBuffer()
    @test_throws Test.FallbackTestSetException("Test run finished with errors") begin
        runtests(["--verbose"]; custom_tests, stdout=io, stderr=io)
    end

    str = String(take!(io))
    @test contains(str, r"basic .+ started at")
    @test contains(str, r"failing test .+ failed at")
    @test contains(str, "FAILURE")
    @test contains(str, "Test Failed")
    @test contains(str, "1 == 2")
end

@testset "throwing test" begin
    custom_tests = Dict(
        "throwing test" => quote
            error("This test throws an error")
        end
    )

    io = IOBuffer()
    @test_throws Test.FallbackTestSetException("Test run finished with errors") begin
        runtests(["--verbose"]; custom_tests, stdout=io, stderr=io)
    end

    str = String(take!(io))
    @test contains(str, r"basic .+ started at")
    @test contains(str, r"throwing test .+ failed at")
    @test contains(str, "FAILURE")
    @test contains(str, "Error During Test")
    @test contains(str, "This test throws an error")
end

@testset "test output" begin
    custom_tests = Dict(
        "output" => quote
            println("This is some output from the test")
        end
    )

    io = IOBuffer()
    runtests(["--verbose"]; custom_tests, stdout=io, stderr=io)

    str = String(take!(io))
    @test contains(str, r"output .+ started at")
    @test contains(str, r"This is some output from the test")
    @test contains(str, "SUCCESS")
end

end
