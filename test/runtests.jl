using ParallelTestRunner
using Test, IOCapture

@testset "ParallelTestRunner" verbose=true begin

@testset "basic test" begin
    println()
    println("Showing the output of one test run:")
    println("-"^80)
    c = IOCapture.capture(passthrough=true, color=true) do
        runtests(["--verbose"])
    end
    println("-"^80)
    println()
    @test contains(c.output, r"basic.*started at")
    @test contains(c.output, "SUCCESS")
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
    c = IOCapture.capture() do
        runtests(["--verbose"]; init_code, custom_tests)
    end
    @test contains(c.output, r"basic .+ started at")
    @test contains(c.output, r"custom .+ started at")
    @test contains(c.output, "SUCCESS")
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
    c = IOCapture.capture() do
        runtests(["--verbose"]; test_worker, custom_tests)
    end
    @test contains(c.output, r"basic .+ started at")
    @test contains(c.output, r"needs env var .+ started at")
    @test contains(c.output, r"doesn't need env var .+ started at")
    @test contains(c.output, "SUCCESS")
end

@testset "failing test" begin
    custom_tests = Dict(
        "failing test" => quote
            @test 1 == 2
        end
    )
    c = IOCapture.capture(rethrow=Union{}) do
        runtests(["--verbose"]; custom_tests)
    end
    @test contains(c.output, r"basic .+ started at")
    @test contains(c.output, r"failing test .+ failed at")
    @test contains(c.output, "FAILURE")
    @test contains(c.output, "1 == 2")
    @test c.error
    @test c.value == Test.FallbackTestSetException("Test run finished with errors")
end

@testset "throwing test" begin
    custom_tests = Dict(
        "throwing test" => quote
            error("This test throws an error")
        end
    )
    c = IOCapture.capture(rethrow=Union{}) do
        runtests(["--verbose"]; custom_tests)
    end
    @test contains(c.output, r"basic .+ started at")
    @test contains(c.output, r"throwing test .+ failed at")
    @test contains(c.output, "FAILURE")
    @test contains(c.output, "Got exception outside of a @test")
    @test c.error
    @test c.value == Test.FallbackTestSetException("Test run finished with errors")
end

end
