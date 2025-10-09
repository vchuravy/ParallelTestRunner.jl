using ParallelTestRunner

pushfirst!(ARGS, "--verbose")

custom_tests = Dict(
    "whatever" => Returns(nothing)
)

runtests(ARGS; custom_tests)
