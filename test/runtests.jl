using ParallelTestRunner

pushfirst!(ARGS, "--verbose")

runtests(ARGS)
