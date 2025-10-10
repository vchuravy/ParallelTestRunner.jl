using ParallelTestRunner
using Test

pushfirst!(ARGS, "--verbose")

runtests(ARGS)

# custom tests, and initialization code
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
runtests(ARGS; init_code, custom_tests)

# custom worker
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
runtests(ARGS; test_worker, custom_tests)

# failing test
custom_tests = Dict(
    "failing test" => quote
        @test 1 == 2
    end
)
@test_throws Test.FallbackTestSetException("Test run finished with errors") runtests(ARGS; custom_tests)

# throwing test
custom_tests = Dict(
    "throwing test" => quote
        error("This test throws an error")
    end
)
@test_throws Test.FallbackTestSetException("Test run finished with errors") runtests(ARGS; custom_tests)
