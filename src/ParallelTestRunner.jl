module ParallelTestRunner

export runtests

using Distributed
using Dates
import REPL
using Printf: @sprintf
using Base.Filesystem: path_separator
import Test
import Random

#Always set the max rss so that if tests add large global variables (which they do) we don't make the GC's life too hard
if Sys.WORD_SIZE == 64
    const JULIA_TEST_MAXRSS_MB = 3800
else
    # Assume that we only have 3.5GB available to a single process, and that a single
    # test can take up to 2GB of RSS.  This means that we should instruct the test
    # framework to restart any worker that comes into a test set with 1.5GB of RSS.
    const JULIA_TEST_MAXRSS_MB = 1536
end

const max_worker_rss = JULIA_TEST_MAXRSS_MB * 2^20

# parse some command-line arguments
function extract_flag!(args, flag, default = nothing; typ = typeof(default))
    for f in args
        if startswith(f, flag)
            # Check if it's just `--flag` or if it's `--flag=foo`
            if f != flag
                val = split(f, '=')[2]
                if !(typ === Nothing || typ <: AbstractString)
                    val = parse(typ, val)
                end
            else
                val = default
            end

            # Drop this value from our args
            filter!(x -> x != f, args)
            return (true, val)
        end
    end
    return (false, default)
end

function with_testset(f, testset)
    @static if VERSION >= v"1.13.0-DEV.1044"
        Test.@with_testset testset f()
    else
        Test.push_testset(testset)
        try
            f()
        finally
            Test.pop_testset()
        end
    end
    return nothing
end

if VERSION >= v"1.13.0-DEV.1044"
    using Base.ScopedValues
end

abstract type AbstractTestRecord end

struct TestRecord <: AbstractTestRecord
    test::Any
    time::Float64
    bytes::Int
    gctime::Float64
    rss::UInt
end

function memory_usage(rec::TestRecord)
    return rec.rss
end

struct TestIOContext
    io::IO
    lock::ReentrantLock
    name_align::Int
    elapsed_align::Int
    gc_align::Int
    percent_align::Int
    alloc_align::Int
    rss_align::Int
end

function test_IOContext(::Type{TestRecord}, io::IO, lock::ReentrantLock, name_align::Int)
    elapsed_align = textwidth("Time (s)")
    gc_align = textwidth("GC (s)")
    percent_align = textwidth("GC %")
    alloc_align = textwidth("Alloc (MB)")
    rss_align = textwidth("RSS (MB)")

    return TestIOContext(
        io, lock, name_align, elapsed_align, gc_align, percent_align,
        alloc_align, rss_align
    )
end

function print_header(::Type{TestRecord}, ctx::TestIOContext, testgroupheader, workerheader)
    printstyled(ctx.io, " "^(ctx.name_align + textwidth(testgroupheader) - 3), " | ")
    printstyled(ctx.io, "         | ---------------- CPU ---------------- |\n", color = :white)
    printstyled(ctx.io, testgroupheader, color = :white)
    printstyled(ctx.io, lpad(workerheader, ctx.name_align - textwidth(testgroupheader) + 1), " | ", color = :white)
    printstyled(ctx.io, "Time (s) |  GC (s) | GC % | Alloc (MB) | RSS (MB) |\n", color = :white)
    return nothing
end

function print_testworker_stats(test, wrkr, record::TestRecord, ctx::TestIOContext)
    lock(ctx.lock)
    return try
        printstyled(ctx.io, test, color = :white)
        printstyled(ctx.io, lpad("($wrkr)", ctx.name_align - textwidth(test) + 1, " "), " | ", color = :white)
        time_str = @sprintf("%7.2f", record.time)
        printstyled(ctx.io, lpad(time_str, ctx.elapsed_align, " "), " | ", color = :white)

        gc_str = @sprintf("%5.2f", record.gctime)
        printstyled(ctx.io, lpad(gc_str, ctx.gc_align, " "), " | ", color = :white)
        percent_str = @sprintf("%4.1f", 100 * record.gctime / record.time)
        printstyled(ctx.io, lpad(percent_str, ctx.percent_align, " "), " | ", color = :white)
        alloc_str = @sprintf("%5.2f", record.bytes / 2^20)
        printstyled(ctx.io, lpad(alloc_str, ctx.alloc_align, " "), " | ", color = :white)

        rss_str = @sprintf("%5.2f", record.rss / 2^20)
        printstyled(ctx.io, lpad(rss_str, ctx.rss_align, " "), " |\n", color = :white)
    finally
        unlock(ctx.lock)
    end
end


## entry point

function runtest(::Type{TestRecord}, f, name)
    function inner()
        # generate a temporary module to execute the tests in
        mod_name = Symbol("Test", rand(1:100), "Main_", replace(name, '/' => '_'))
        mod = @eval(Main, module $mod_name end)
        @eval(mod, import ParallelTestRunner: Test, Random)
        @eval(mod, using .Test, .Random)

        let id = myid()
            wait(@spawnat 1 print_testworker_started(name, id))
        end

        ex = quote
            GC.gc(true)
            Random.seed!(1)

            res = @timed @testset $name begin
                $f()
            end
            (res...,)
        end
        data = Core.eval(mod, ex)

        #data[1] is the testset

        # process results
        rss = Sys.maxrss()
        if VERSION >= v"1.11.0-DEV.1529"
            tc = Test.get_test_counts(data[1])
            passes, fails, error, broken, c_passes, c_fails, c_errors, c_broken =
                tc.passes, tc.fails, tc.errors, tc.broken, tc.cumulative_passes,
                tc.cumulative_fails, tc.cumulative_errors, tc.cumulative_broken
        else
            passes, fails, errors, broken, c_passes, c_fails, c_errors, c_broken =
                Test.get_test_counts(data[1])
        end
        if data[1].anynonpass == false
            data = (
                (passes + c_passes, broken + c_broken),
                data[2],
                data[3],
                data[4],
            )
        end
        res = TestRecord(data..., rss)

        GC.gc(true)
        return res
    end

    res = @static if VERSION >= v"1.13.0-DEV.1044"
        @with Test.TESTSET_PRINT_ENABLE => false begin
            inner()
        end
    else
        old_print_setting = Test.TESTSET_PRINT_ENABLE[]
        Test.TESTSET_PRINT_ENABLE[] = false
        try
            inner()
        finally
            Test.TESTSET_PRINT_ENABLE[] = old_print_setting
        end
    end
    return res
end

# This is an internal function, not to be used by end users.  The keyword
# arguments are only for testing purposes.
"""
    default_njobs()

Determine default number of parallel jobs.
"""
function default_njobs(; cpu_threads = Sys.CPU_THREADS, free_memory = Sys.free_memory())
    jobs = cpu_threads
    memory_jobs = Int64(free_memory) ÷ (2 * 2^30)
    return max(1, min(jobs, memory_jobs))
end

"""
    runtests(ARGS; testfilter = Returns(true), RecordType = TestRecord, custom_tests = Dict())

Run Julia tests in parallel across multiple worker processes.

## Arguments

The primary argument is a command line arguments array, typically from `Base.ARGS`. When you
run the tests with `Pkg.test`, this can be changed with the `test_args` keyword argument.

Several keyword arguments are also supported:

- `testfilter`: Optional function to filter which tests to run (default: run all tests)
- `RecordType`: Type of test record to use for tracking test results (default: `TestRecord`)
- `custom_tests`: Optional dictionary of custom tests, mapping test names to a zero-argument
  function

## Command Line Options

- `--help`: Show usage information and exit
- `--list`: List all available test files and exit
- `--verbose`: Print more detailed information during test execution
- `--quickfail`: Stop the entire test run as soon as any test fails
- `--jobs=N`: Use N worker processes (default: based on CPU threads and available memory)
- `TESTS...`: Filter tests by name, matched using `startswith`

## Behavior

- Automatically discovers all `.jl` files in the test directory (excluding `setup.jl` and
  `runtests.jl`)
- Sorts tests by file size (largest first) for load balancing
- Launches worker processes with appropriate Julia flags for testing
- Monitors memory usage and recycles workers that exceed memory limits
- Provides real-time progress output with timing and memory statistics
- Handles interruptions gracefully (Ctrl+C)
- Returns nothing, but throws `Test.FallbackTestSetException` if any tests fail

## Examples

```julia
# Run all tests with default settings
runtests(ARGS)

# Run only tests matching "integration"
runtests(["integration"])

# Run with custom filter function
runtests(ARGS, test -> occursin("unit", test))

# Use custom test record type
runtests(ARGS, Returns(true), MyCustomTestRecord)
```

## Memory Management

Workers are automatically recycled when they exceed memory limits to prevent out-of-memory
issues during long test runs. The memory limit is set based on system architecture.
"""
function runtests(ARGS; testfilter = Returns(true), RecordType = TestRecord,
                  custom_tests::Dict{String}=Dict{String}())
    do_help, _ = extract_flag!(ARGS, "--help")
    if do_help
        println(
            """
            Usage: runtests.jl [--help] [--list] [--jobs=N] [TESTS...]

               --help             Show this text.
               --list             List all available tests.
               --verbose          Print more information during testing.
               --quickfail        Fail the entire run as soon as a single test errored.
               --jobs=N           Launch `N` processes to perform tests.

               Remaining arguments filter the tests that will be executed."""
        )
        exit(0)
    end
    set_jobs, jobs = extract_flag!(ARGS, "--jobs"; typ = Int)
    do_verbose, _ = extract_flag!(ARGS, "--verbose")
    do_quickfail, _ = extract_flag!(ARGS, "--quickfail")
    do_list, _ = extract_flag!(ARGS, "--list")
    ## no options should remain
    optlike_args = filter(startswith("-"), ARGS)
    if !isempty(optlike_args)
        error("Unknown test options `$(join(optlike_args, " "))` (try `--help` for usage instructions)")
    end

    WORKDIR = pwd()

    # choose tests
    tests = []
    test_runners = Dict()
    ## custom tests by the user
    for (name, runner) in custom_tests
        push!(tests, name)
        test_runners[name] = runner
    end
    ## files in the test folder
    for (rootpath, dirs, files) in walkdir(WORKDIR)
        # find Julia files
        filter!(files) do file
            endswith(file, ".jl") && file !== "setup.jl" && file !== "runtests.jl"
        end
        isempty(files) && continue

        # strip extension
        files = map(files) do file
            file[1:(end - 3)]
        end

        # prepend subdir
        subdir = relpath(rootpath, WORKDIR)
        if subdir != "."
            files = map(files) do file
                joinpath(subdir, file)
            end
        end

        # unify path separators
        files = map(files) do file
            replace(file, path_separator => '/')
        end

        append!(tests, files)
        for file in files
            test_runners[file] = ()->Main.include(joinpath(WORKDIR, file * ".jl"))
        end
    end
    sort!(tests; by = (file) -> stat(joinpath(WORKDIR, file * ".jl")).size, rev = true)
    ## finalize
    unique!(tests)

    # list tests, if requested
    if do_list
        println("Available tests:")
        for test in sort(tests)
            println(" - $test")
        end
        exit(0)
    end

    # filter tests
    if isempty(ARGS)
        filter!(testfilter, tests)
    else
        # let the user filter
        filter!(tests) do test
            any(arg -> startswith(test, arg), ARGS)
        end
    end

    # determine parallelism
    if !set_jobs
        jobs = default_njobs()
    end
    @info "Running $jobs tests in parallel. If this is too many, specify the `--jobs=N` argument to the tests, or set the `JULIA_CPU_THREADS` environment variable."

    # add workers
    test_exeflags = Base.julia_cmd()
    filter!(test_exeflags.exec) do c
        return !(startswith(c, "--depwarn") || startswith(c, "--check-bounds"))
    end
    push!(test_exeflags.exec, "--check-bounds=yes")
    push!(test_exeflags.exec, "--startup-file=no")
    push!(test_exeflags.exec, "--depwarn=yes")
    push!(test_exeflags.exec, "--project=$(Base.active_project())")
    test_exename = popfirst!(test_exeflags.exec)
    function addworker(X; kwargs...)
        exename = test_exename

        return withenv("JULIA_NUM_THREADS" => 1, "OPENBLAS_NUM_THREADS" => 1) do
            procs = addprocs(X; exename = exename, exeflags = test_exeflags, kwargs...)
            Distributed.remotecall_eval(
                Main, procs, quote
                    import ParallelTestRunner
                    # TODO: Should we import Test for the user here?
                    import ParallelTestRunner: Test
                    using .Test
                end
            )
            if ispath(joinpath(WORKDIR, "setup.jl"))
                @everywhere procs include($(joinpath(WORKDIR, "setup.jl")))
            end
            procs
        end
    end
    addworker(min(jobs, length(tests)))

    # pretty print information about gc and mem usage
    testgroupheader = "Test"
    workerheader = "(Worker)"
    name_align = maximum(
        [
            textwidth(testgroupheader) + textwidth(" ") +
                textwidth(workerheader); map(
                x -> textwidth(x) +
                    3 + ndigits(nworkers()), tests
            )
        ]
    )

    elapsed_align = textwidth("Time (s)")
    print_lock = stdout isa Base.LibuvStream ? stdout.lock : ReentrantLock()
    if stderr isa Base.LibuvStream
        stderr.lock = print_lock
    end

    io_ctx = test_IOContext(RecordType, stdout, print_lock, name_align)
    print_header(RecordType, io_ctx, testgroupheader, workerheader)

    global print_testworker_started = (name, wrkr) -> begin
        if do_verbose
            lock(print_lock)
            try
                printstyled(name, color = :white)
                printstyled(
                    lpad("($wrkr)", name_align - textwidth(name) + 1, " "), " |",
                    " "^elapsed_align, "started at $(now())\n", color = :white
                )
            finally
                unlock(print_lock)
            end
        end
    end
    function print_testworker_errored(name, wrkr)
        lock(print_lock)
        return try
            printstyled(name, color = :red)
            printstyled(
                lpad("($wrkr)", name_align - textwidth(name) + 1, " "), " |",
                " "^elapsed_align, " failed at $(now())\n", color = :red
            )
        finally
            unlock(print_lock)
        end
    end

    # run tasks
    t0 = now()
    results = []
    all_tasks = Task[]
    try
        # Monitor stdin and kill this task on ^C
        # but don't do this on Windows, because it may deadlock in the kernel
        t = current_task()
        running_tests = Dict{String, DateTime}()
        if !Sys.iswindows() && isa(stdin, Base.TTY)
            stdin_monitor = @async begin
                term = REPL.Terminals.TTYTerminal("xterm", stdin, stdout, stderr)
                try
                    REPL.Terminals.raw!(term, true)
                    while true
                        c = read(term, Char)
                        if c == '\x3'
                            Base.throwto(t, InterruptException())
                            break
                        elseif c == '?'
                            println("Currently running: ")
                            tests = sort(collect(running_tests), by = x -> x[2])
                            foreach(tests) do (test, date)
                                println(test, " (running for ", round(now() - date, Minute), ")")
                            end
                        end
                    end
                catch e
                    isa(e, InterruptException) || rethrow()
                finally
                    REPL.Terminals.raw!(term, false)
                end
            end
        end
        @sync begin
            function recycle_worker(p)
                rmprocs(p, waitfor = 30)

                return nothing
            end

            for p in workers()
                @async begin
                    push!(all_tasks, current_task())
                    while length(tests) > 0
                        test = popfirst!(tests)

                        # sometimes a worker failed, and we need to spawn a new one
                        if p === nothing
                            p = addworker(1)[1]
                        end
                        wrkr = p

                        local resp

                        # run the test
                        running_tests[test] = now()
                        try
                            resp = remotecall_fetch(runtest, wrkr, RecordType, test_runners[test], test)
                        catch e
                            isa(e, InterruptException) && return
                            resp = Any[e]
                        end
                        delete!(running_tests, test)
                        push!(results, (test, resp))

                        # act on the results
                        if resp isa AbstractTestRecord
                            print_testworker_stats(test, wrkr, resp::RecordType, io_ctx)

                            if memory_usage(resp) > max_worker_rss
                                # the worker has reached the max-rss limit, recycle it
                                # so future tests start with a smaller working set
                                p = recycle_worker(p)
                            end
                        else
                            @assert resp[1] isa Exception
                            print_testworker_errored(test, wrkr)
                            do_quickfail && Base.throwto(t, InterruptException())

                            # the worker encountered some failure, recycle it
                            # so future tests get a fresh environment
                            p = recycle_worker(p)
                        end
                    end

                    if p !== nothing
                        recycle_worker(p)
                    end
                end
            end
        end
    catch e
        isa(e, InterruptException) || rethrow()
        # If the test suite was merely interrupted, still print the
        # summary, which can be useful to diagnose what's going on
        foreach(
            task -> begin
                istaskstarted(task) || return
                istaskdone(task) && return
                try
                    schedule(task, InterruptException(); error = true)
                catch ex
                    @error "InterruptException" exception = ex, catch_backtrace()
                end
            end, all_tasks
        )
        for t in all_tasks
            # NOTE: we can't just wait, but need to discard the exception,
            #       because the throwto for --quickfail also kills the worker.
            try
                wait(t)
            catch e
                showerror(stderr, e)
            end
        end
    finally
        if @isdefined stdin_monitor
            schedule(stdin_monitor, InterruptException(); error = true)
        end
    end
    t1 = now()
    elapsed = canonicalize(Dates.CompoundPeriod(t1 - t0))
    println("Testing finished in $elapsed")

    # construct a testset to render the test results
    o_ts = Test.DefaultTestSet("Overall")
    with_testset(o_ts) do
        completed_tests = Set{String}()
        for (testname, res) in results
            if res isa AbstractTestRecord
                resp = res.test
            else
                resp = res[1]
            end
            push!(completed_tests, testname)
            if isa(resp, Test.DefaultTestSet)
                with_testset(resp) do
                    Test.record(o_ts, resp)
                end
            elseif isa(resp, Tuple{Int, Int})
                fake = Test.DefaultTestSet(testname)
                for i in 1:resp[1]
                    Test.record(fake, Test.Pass(:test, nothing, nothing, nothing, nothing))
                end
                for i in 1:resp[2]
                    Test.record(fake, Test.Broken(:test, nothing))
                end
                with_testset(fake) do
                    Test.record(o_ts, fake)
                end
            elseif isa(resp, RemoteException) && isa(resp.captured.ex, Test.TestSetException)
                println("Worker $(resp.pid) failed running test $(testname):")
                Base.showerror(stdout, resp.captured)
                println()
                fake = Test.DefaultTestSet(testname)
                for i in 1:resp.captured.ex.pass
                    Test.record(fake, Test.Pass(:test, nothing, nothing, nothing, nothing))
                end
                for i in 1:resp.captured.ex.broken
                    Test.record(fake, Test.Broken(:test, nothing))
                end
                for t in resp.captured.ex.errors_and_fails
                    Test.record(fake, t)
                end
                with_testset(fake) do
                    Test.record(o_ts, fake)
                end
            else
                if !isa(resp, Exception)
                    resp = ErrorException(string("Unknown result type : ", typeof(resp)))
                end
                # If this test raised an exception that is not a remote testset exception,
                # i.e. not a RemoteException capturing a TestSetException that means
                # the test runner itself had some problem, so we may have hit a segfault,
                # deserialization errors or something similar.  Record this testset as Errored.
                fake = Test.DefaultTestSet(testname)
                Test.record(fake, Test.Error(:nontest_error, testname, nothing, Base.ExceptionStack([(exception = resp, backtrace = [])]), LineNumberNode(1)))
                with_testset(fake) do
                    Test.record(o_ts, fake)
                end
            end
        end
        for test in tests
            (test in completed_tests) && continue
            fake = Test.DefaultTestSet(test)
            Test.record(fake, Test.Error(:test_interrupted, test, nothing, Base.ExceptionStack([(exception = "skipped", backtrace = [])]), LineNumberNode(1)))
            with_testset(fake) do
                Test.record(o_ts, fake)
            end
        end
    end
    println()
    Test.print_test_results(o_ts, 1)
    if (VERSION >= v"1.13.0-DEV.1037" && !Test.anynonpass(o_ts)) ||
            (VERSION < v"1.13.0-DEV.1037" && !o_ts.anynonpass)
        println("    \033[32;1mSUCCESS\033[0m")
    else
        println("    \033[31;1mFAILURE\033[0m\n")
        Test.print_test_errors(o_ts)
        throw(Test.FallbackTestSetException("Test run finished with errors"))
    end
    return nothing
end # runtests

end # module ParallelTestRunner
