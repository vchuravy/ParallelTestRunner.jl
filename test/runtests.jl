using ParallelTestRunner

pushfirst!(ARGS, "--verbose")

init_code = quote
    using Test
    should_be_defined() = true

    macro should_also_be_defined()
        return :(true)
    end
end

runtests(ARGS; init_code)

custom_tests = Dict(
    "custom" => quote
        @test should_be_defined()
        @test @should_also_be_defined()
    end
)
runtests(ARGS; init_code, custom_tests)
