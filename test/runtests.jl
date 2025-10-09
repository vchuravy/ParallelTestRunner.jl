using ParallelTestRunner

pushfirst!(ARGS, "--verbose")

init_code = quote
    using Test
    should_be_defined() = true
end

runtests(ARGS; init_code)

custom_tests = Dict(
    "custom" => quote
        @test should_be_defined()
    end
)
runtests(ARGS; init_code, custom_tests)
