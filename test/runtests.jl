using ParallelTestRunner
using Test

cd(@__DIR__)

@testset "ParallelTestRunner" verbose=true begin

@testset "basic test" begin
    io = IOBuffer()
    io_color = IOContext(io, :color => true)
    runtests(ParallelTestRunner, ["--verbose"]; stdout=io_color, stderr=io_color)
    str = String(take!(io))

    println()
    println("Showing the output of one test run:")
    println("-"^80)
    print(str)
    println("-"^80)
    println()

    @test contains(str, r"basic .+ started at")
    @test contains(str, "SUCCESS")

    @test isfile(ParallelTestRunner.get_history_file(ParallelTestRunner))
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
    runtests(ParallelTestRunner, ["--verbose"]; init_code, custom_tests, stdout=io, stderr=io)

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
    runtests(ParallelTestRunner, ["--verbose"]; test_worker, custom_tests, stdout=io, stderr=io)

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
    error_line = @__LINE__() - 3

    io = IOBuffer()
    @test_throws Test.FallbackTestSetException("Test run finished with errors") begin
        runtests(ParallelTestRunner, ["--verbose"]; custom_tests, stdout=io, stderr=io)
    end

    str = String(take!(io))
    @test contains(str, r"basic .+ started at")
    @test contains(str, r"failing test .+ failed at")
    @test contains(str, "$(basename(@__FILE__)):$error_line")
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
    error_line = @__LINE__() - 3

    io = IOBuffer()
    @test_throws Test.FallbackTestSetException("Test run finished with errors") begin
        runtests(ParallelTestRunner, ["--verbose"]; custom_tests, stdout=io, stderr=io)
    end

    str = String(take!(io))
    @test contains(str, r"basic .+ started at")
    @test contains(str, r"throwing test .+ failed at")
    @test contains(str, "$(basename(@__FILE__)):$error_line")
    @test contains(str, "FAILURE")
    @test contains(str, "Error During Test")
    @test contains(str, "This test throws an error")
end

@testset "crashing test" begin
    custom_tests = Dict(
        "abort" => quote
            abort() = ccall(:abort, Nothing, ())
            abort()
        end
    )

    print("""NOTE: The next test is expected to crash a worker process,
                   which may print some output to the terminal.
             """)
    io = IOBuffer()
    @test_throws Test.FallbackTestSetException("Test run finished with errors") begin
        runtests(ParallelTestRunner, ["--verbose"]; custom_tests, stdout=io, stderr=io)
    end
    println()

    str = String(take!(io))
    @test contains(str, r"abort .+ started at")
    @test contains(str, r"abort .+ crashed at")
    @test contains(str, "FAILURE")
    @test contains(str, "Error During Test")
    @test contains(str, "Malt.TerminatedWorkerException")
end

@testset "test output" begin
    custom_tests = Dict(
        "output" => quote
            println("This is some output from the test")
        end
    )

    io = IOBuffer()
    runtests(ParallelTestRunner, ["--verbose"]; custom_tests, stdout=io, stderr=io)

    str = String(take!(io))
    @test contains(str, r"output .+ started at")
    @test contains(str, r"This is some output from the test")
    @test contains(str, "SUCCESS")
end

@testset "warnings" begin
    custom_tests = Dict(
        "warning" => quote
            @test_warn "3.0" @warn "3.0"
        end
    )

    io = IOBuffer()
    runtests(ParallelTestRunner, ["--verbose"]; custom_tests, stdout=io, stderr=io)

    str = String(take!(io))
    @test contains(str, r"warning .+ started at")
    @test contains(str, "SUCCESS")
end

end
