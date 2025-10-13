module ParallelTestRunner

export runtests, addworkers, addworker

using Distributed
using Dates
import REPL
using Printf: @sprintf
using Base.Filesystem: path_separator
import Test
import Random
import IOCapture

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
    output::String
    time::Float64
    bytes::UInt64
    gctime::Float64
    rss::UInt64
end

function memory_usage(rec::TestRecord)
    return rec.rss
end


#
# overridable I/O context for pretty-printing
#

struct TestIOContext
    stdout::IO
    stderr::IO
    lock::ReentrantLock
    name_align::Int
    elapsed_align::Int
    gc_align::Int
    percent_align::Int
    alloc_align::Int
    rss_align::Int
end

function test_IOContext(::Type{TestRecord}, stdout::IO, stderr::IO, lock::ReentrantLock, name_align::Int)
    elapsed_align = textwidth("Time (s)")
    gc_align = textwidth("GC (s)")
    percent_align = textwidth("GC %")
    alloc_align = textwidth("Alloc (MB)")
    rss_align = textwidth("RSS (MB)")

    return TestIOContext(
        stdout, stderr, lock, name_align, elapsed_align, gc_align, percent_align,
        alloc_align, rss_align
    )
end

function print_header(::Type{TestRecord}, ctx::TestIOContext, testgroupheader, workerheader)
    lock(ctx.lock)
    try
        printstyled(ctx.stdout, " "^(ctx.name_align + textwidth(testgroupheader) - 3), " | ")
        printstyled(ctx.stdout, "         | ---------------- CPU ---------------- |\n", color = :white)
        printstyled(ctx.stdout, testgroupheader, color = :white)
        printstyled(ctx.stdout, lpad(workerheader, ctx.name_align - textwidth(testgroupheader) + 1), " | ", color = :white)
        printstyled(ctx.stdout, "Time (s) | GC (s) | GC % | Alloc (MB) | RSS (MB) |\n", color = :white)
        flush(ctx.stdout)
    finally
        unlock(ctx.lock)
    end
end

function print_test_started(::Type{TestRecord}, wrkr, test, ctx::TestIOContext)
    lock(ctx.lock)
    try
        printstyled(ctx.stdout, test, color = :white)
        printstyled(
            ctx.stdout,
            lpad("($wrkr)", ctx.name_align - textwidth(test) + 1, " "), " |",
            " "^ctx.elapsed_align, "started at $(now())\n", color = :white
        )
        flush(ctx.stdout)
    finally
        unlock(ctx.lock)
    end
end

function print_test_finished(test, wrkr, record::TestRecord, ctx::TestIOContext)
    lock(ctx.lock)
    try
        printstyled(ctx.stdout, test, color = :white)
        printstyled(ctx.stdout, lpad("($wrkr)", ctx.name_align - textwidth(test) + 1, " "), " | ", color = :white)
        time_str = @sprintf("%7.2f", record.time)
        printstyled(ctx.stdout, lpad(time_str, ctx.elapsed_align, " "), " | ", color = :white)

        gc_str = @sprintf("%5.2f", record.gctime)
        printstyled(ctx.stdout, lpad(gc_str, ctx.gc_align, " "), " | ", color = :white)
        percent_str = @sprintf("%4.1f", 100 * record.gctime / record.time)
        printstyled(ctx.stdout, lpad(percent_str, ctx.percent_align, " "), " | ", color = :white)
        alloc_str = @sprintf("%5.2f", record.bytes / 2^20)
        printstyled(ctx.stdout, lpad(alloc_str, ctx.alloc_align, " "), " | ", color = :white)

        rss_str = @sprintf("%5.2f", record.rss / 2^20)
        printstyled(ctx.stdout, lpad(rss_str, ctx.rss_align, " "), " |\n", color = :white)

        for line in eachline(IOBuffer(record.output))
            println(ctx.stdout, " "^(ctx.name_align + 2), "| ", line)
        end

        flush(ctx.stdout)
    finally
        unlock(ctx.lock)
    end
end

function print_test_errorred(::Type{TestRecord}, wrkr, test, ctx::TestIOContext)
    lock(ctx.lock)
    try
        printstyled(ctx.stderr, test, color = :red)
        printstyled(
            ctx.stderr,
            lpad("($wrkr)", ctx.name_align - textwidth(test) + 1, " "), " |",
            " "^ctx.elapsed_align, " failed at $(now())\n", color = :red
        )

        flush(ctx.stderr)
    finally
        unlock(ctx.lock)
    end
end


#
# entry point
#

function runtest(::Type{TestRecord}, f, name, init_code)
    function inner()
        # generate a temporary module to execute the tests in
        mod_name = Symbol("Test", rand(1:100), "Main_", replace(name, '/' => '_'))
        mod = @eval(Main, module $mod_name end)
        @eval(mod, import ParallelTestRunner: Test, Random, IOCapture)
        @eval(mod, using .Test, .Random)

        Core.eval(mod, init_code)

        data = @eval mod begin
            GC.gc(true)
            Random.seed!(1)

            res = @timed IOCapture.capture() do
                @testset $name begin
                    $f
                end
            end
            captured = res.value
            (; testset=captured.value, captured.output, res.time, res.bytes, res.gctime)
        end

        # process results
        rss = Sys.maxrss()
        if VERSION >= v"1.11.0-DEV.1529"
            tc = Test.get_test_counts(data.testset)
            passes, fails, error, broken, c_passes, c_fails, c_errors, c_broken =
                tc.passes, tc.fails, tc.errors, tc.broken, tc.cumulative_passes,
                tc.cumulative_fails, tc.cumulative_errors, tc.cumulative_broken
        else
            passes, fails, errors, broken, c_passes, c_fails, c_errors, c_broken =
                Test.get_test_counts(data.testset)
        end
        if !data.testset.anynonpass
            data = (;
                result=(passes + c_passes, broken + c_broken),
                data.output,
                data.time,
                data.bytes,
                data.gctime,
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

function test_exe()
    test_exeflags = Base.julia_cmd()
    filter!(test_exeflags.exec) do c
        !(startswith(c, "--depwarn") || startswith(c, "--check-bounds"))
    end
    push!(test_exeflags.exec, "--check-bounds=yes")
    push!(test_exeflags.exec, "--startup-file=no")
    push!(test_exeflags.exec, "--depwarn=yes")
    push!(test_exeflags.exec, "--project=$(Base.active_project())")
    return test_exeflags
end

"""
    addworkers(X; kwargs...)

Add `X` worker processes, with additional keyword arguments passed to `Distributed.addprocs`.
"""
function addworkers(X; kwargs...)
    exe = test_exe()
    exename = exe[1]
    exeflags = exe[2:end]

    return withenv("JULIA_NUM_THREADS" => 1, "OPENBLAS_NUM_THREADS" => 1) do
        procs = addprocs(X; exename, exeflags, kwargs...)
        Distributed.remotecall_eval(
            Main, procs, quote
                import ParallelTestRunner
            end
        )
        procs
    end
end
addworker(; kwargs...) = addworkers(1; kwargs...)[1]

function recycle_worker(p)
    rmprocs(p, waitfor = 30)

    return nothing
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
- `custom_tests`: Optional dictionary of custom tests, mapping test names to expressions.
- `init_code`: Code use to initialize each test's sandbox module (e.g., import auxiliary
  packages, define constants, etc).
- `test_worker`: Optional function that takes a test name and returns a specific worker.
  When returning `nothing`, the test will be assigned to any available default worker.

## Command Line Options

- `--help`: Show usage information and exit
- `--list`: List all available test files and exit
- `--verbose`: Print more detailed information during test execution
- `--quickfail`: Stop the entire test run as soon as any test fails
- `--jobs=N`: Use N worker processes (default: based on CPU threads and available memory)
- `TESTS...`: Filter tests by name, matched using `startswith`
- `stdout` and `stderr`: I/O streams to write to (default: `Base.stdout` and `Base.stderr`)

## Behavior

- Automatically discovers all `.jl` files in the test directory (excluding `runtests.jl`)
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
                  custom_tests::Dict{String, Expr}=Dict{String, Expr}(), init_code = :(),
                  test_worker = Returns(nothing), stdout = Base.stdout, stderr = Base.stderr)
    #
    # set-up
    #

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
            endswith(file, ".jl") && file !== "runtests.jl"
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
            test_runners[file] = quote
                include($(joinpath(WORKDIR, file * ".jl")))
            end
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
    println(stdout, "Running $jobs tests in parallel. If this is too many, specify the `--jobs=N` argument to the tests, or set the `JULIA_CPU_THREADS` environment variable.")

    # add workers
    addworkers(min(jobs, length(tests)))

    t0 = time()
    results = []
    tasks = Task[]
    running_tests = Dict{String, Tuple{Int, Float64}}()  # test => (worker, start_time)
    test_lock = ReentrantLock() # to protect crucial access to tests and running_tests

    done = false
    function stop_work()
        if !done
            done = true
            for task in tasks
                task == current_task() && continue
                Base.istaskdone(task) && continue
                try; schedule(task, InterruptException(); error=true); catch; end
            end
        end
    end


    #
    # input
    #

    # Keyboard monitor (for more reliable CTRL-C handling)
    if isa(stdin, Base.TTY)
        # NOTE: this should be the first task; we really want it to complete
        pushfirst!(tasks, @async begin
            term = REPL.Terminals.TTYTerminal("xterm", stdin, stdout, stderr)
            REPL.Terminals.raw!(term, true)
            try
                while !done
                    c = read(term, Char)
                    if c == '\x3'
                        println(stderr, "\nCaught interrupt, stopping...")
                        stop_work()
                        break
                    end
                end
            finally
                REPL.Terminals.raw!(term, false)
            end
        end)
    end
    # TODO: we have to be _fast_ here, as Pkg.jl only gives us 4 seconds to clean up


    #
    # output
    #

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

    io_ctx = test_IOContext(RecordType, stdout, stderr, print_lock, name_align)
    print_header(RecordType, io_ctx, testgroupheader, workerheader)

    status_lines_visible = Ref(0)

    function clear_status()
        if status_lines_visible[] > 0
            for i in 1:status_lines_visible[]-1
                print(io_ctx.stdout, "\033[1A")  # Move up one line
                print(io_ctx.stdout, "\033[2K")  # Clear entire line
            end
            print(io_ctx.stdout, "\r")  # Move to start of line
            status_lines_visible[] = 0
        end
    end

    function update_status()
        # only draw the status bar on actual terminals
        io_ctx.stdout isa Base.TTY || return

        # only draw if we have something to show
        isempty(running_tests) && return
        completed = length(results)
        total = completed + length(tests) + length(running_tests)

        # line 1: empty line
        line1 = ""

        # line 2: running tests
        test_list = sort(collect(running_tests), by = x -> x[2][2])
        status_parts = map(test_list) do (test, (wrkr, _))
            "$test ($wrkr)"
        end
        line2 = "Running:  " * join(status_parts, ", ")
        ## truncate
        max_width = displaysize(io_ctx.stdout)[2]
        if length(line2) > max_width
            line2 = line2[1:max_width-3] * "..."
        end

        # line 3: progress + ETA
        line3 = "Progress: $completed/$total tests completed"
        if completed > 0
            elapsed_so_far = time() - t0
            avg_time_per_test = elapsed_so_far / completed
            remaining_tests = length(tests) + length(running_tests)
            eta_seconds = avg_time_per_test * remaining_tests
            eta_mins = round(Int, eta_seconds / 60)
            line3 *= " | ETA: ~$eta_mins min"
        end

        # display
        clear_status()
        println(io_ctx.stdout, line1)
        println(io_ctx.stdout, line2)
        print(io_ctx.stdout, line3)
        flush(io_ctx.stdout)
        status_lines_visible[] = 3
    end

    # Message types for the printer channel
    # (:started, test_name, worker_id)
    # (:finished, test_name, worker_id, record)
    # (:errored, test_name, worker_id)
    printer_channel = Channel{Tuple}(100)

    printer_task = @async begin
        last_status_update = Ref(time())
        try
            # XXX: it's possible this task doesn't run, not processing results,
            #      while the execution runners have exited...
            while isopen(printer_channel)
                got_message = false
                while isready(printer_channel)
                    # Try to get a message from the channel (with timeout)
                    msg = take!(printer_channel)
                    got_message = true
                    msg_type = msg[1]

                    if msg_type == :started
                        test_name, wrkr = msg[2], msg[3]

                        # Optionally print verbose started message
                        if do_verbose
                            clear_status()
                            print_test_started(RecordType, wrkr, test_name, io_ctx)
                        end

                    elseif msg_type == :finished
                        test_name, wrkr, record = msg[2], msg[3], msg[4]

                        clear_status()
                        print_test_finished(test_name, wrkr, record, io_ctx)

                    elseif msg_type == :errored
                        test_name, wrkr = msg[2], msg[3]

                        clear_status()
                        print_test_errorred(RecordType, wrkr, test_name, io_ctx)
                    end
                end

                # After a while, display a status line
                if !done && time() - t0 >= 5 && (got_message || (time() - last_status_update[] >= 1))
                    update_status()
                    last_status_update[] = time()
                end
                sleep(0.1)
            end
        catch ex
            isa(ex, InterruptException) || rethrow()
        finally
            if isempty(tests) && isempty(running_tests)
                # XXX: only erase the status if we completed successfully.
                #      in other cases we'll have printed "caught interrupt"
                clear_status()
            end
        end
    end


    #
    # execution
    #

    for p in workers()
        push!(tasks, @async begin
            while length(tests) > 0 && !done
                # if a worker failed, spawn a new one
                if p === nothing
                    p = addworkers(1)[1]
                end

                # get a test to run
                test, wrkr, test_t0 = Base.@lock test_lock begin
                    test = popfirst!(tests)
                    wrkr = something(test_worker(test), p)

                    test_t0 = time()
                    running_tests[test] = (wrkr, test_t0)

                    test, wrkr, test_t0
                end

                # run the test
                put!(printer_channel, (:started, test, wrkr))
                resp = try
                    remotecall_fetch(runtest, wrkr, RecordType, test_runners[test], test, init_code)
                catch e
                    isa(e, InterruptException) && return
                    Any[e]
                end
                test_t1 = time()
                push!(results, (test, resp, test_t0, test_t1))

                # act on the results
                if resp isa AbstractTestRecord
                    put!(printer_channel, (:finished, test, wrkr, resp::RecordType))

                    if memory_usage(resp) > max_worker_rss
                        # the worker has reached the max-rss limit, recycle it
                        # so future tests start with a smaller working set
                        p = recycle_worker(p)
                    end
                else
                    @assert resp[1] isa Exception
                    put!(printer_channel, (:errored, test, wrkr))
                    if do_quickfail
                        stop_work()
                    end

                    # the worker encountered some failure, recycle it
                    # so future tests get a fresh environment
                    p = recycle_worker(p)
                end

                # get rid of the custom worker
                if wrkr != p
                    recycle_worker(wrkr)
                end

                delete!(running_tests, test)
            end

            if p !== nothing
                recycle_worker(p)
            end
        end)
    end


    #
    # finalization
    #

    # monitor tasks for failure so that each one doesn't need a try/catch + stop_work()
    try
        while true
            if any(istaskfailed, tasks)
                println(io_ctx.stderr, "\nCaught an error, stopping...")
                break
            elseif done || Base.@lock(test_lock, isempty(tests) && isempty(running_tests))
                break
            end
            sleep(1)
        end
    catch err
        # in case the sleep got interrupted
        isa(err, InterruptException) || rethrow()
    finally
        stop_work()
    end
    ## `wait()` to actually catch any exceptions
    close(printer_channel)
    wait(printer_task)
    for task in tasks
        try
            wait(task)
        catch err
            # unwrap TaskFailedException
            while isa(err, TaskFailedException)
                err = current_exceptions(err.task)[1].exception
            end

            isa(err, InterruptException) || rethrow()
        end
    end

    # construct a testset to render the test results
    t1 = time()
    o_ts = Test.DefaultTestSet("Overall"; verbose=do_verbose)
    if VERSION < v"1.13.0-DEV.1037"
        o_ts.time_start = t0
        o_ts.time_end = t1
    else
        #@atomic o_ts.time_start = t0
        @atomic o_ts.time_end = t1
    end
    with_testset(o_ts) do
        completed_tests = Set{String}()
        for (testname, res, start, stop) in results
            if res isa AbstractTestRecord
                resp = res.test
            else
                resp = res[1]
            end
            push!(completed_tests, testname)

            # decode or fake a testset
            testset = if isa(resp, Test.DefaultTestSet)
                resp
            elseif isa(resp, Tuple{Int, Int})
                fake = Test.DefaultTestSet(testname)
                for i in 1:resp[1]
                    Test.record(fake, Test.Pass(:test, nothing, nothing, nothing, nothing))
                end
                for i in 1:resp[2]
                    Test.record(fake, Test.Broken(:test, nothing))
                end
                fake
            elseif isa(resp, RemoteException) &&
                   isa(resp.captured.ex, Test.TestSetException)
                println(io_ctx.stderr, "Worker $(resp.pid) failed running test $(testname):")
                Base.showerror(io_ctx.stderr, resp.captured)
                println(io_ctx.stderr)

                fake = Test.DefaultTestSet(testname)
                c = IOCapture.capture() do
                    for i in 1:resp.captured.ex.pass
                        Test.record(fake, Test.Pass(:test, nothing, nothing, nothing, nothing))
                    end
                    for i in 1:resp.captured.ex.broken
                        Test.record(fake, Test.Broken(:test, nothing))
                    end
                    for t in resp.captured.ex.errors_and_fails
                        Test.record(fake, t)
                    end
                end
                print(io_ctx.stdout, c.output)
                fake
            else
                if !isa(resp, Exception)
                    resp = ErrorException(string("Unknown result type : ", typeof(resp)))
                end
                # If this test raised an exception that is not a remote testset exception,
                # i.e. not a RemoteException capturing a TestSetException that means
                # the test runner itself had some problem, so we may have hit a segfault,
                # deserialization errors or something similar.  Record this testset as Errored.
                fake = Test.DefaultTestSet(testname)
                c = IOCapture.capture() do
                    Test.record(fake, Test.Error(:nontest_error, testname, nothing, Base.ExceptionStack([(exception = resp, backtrace = [])]), LineNumberNode(1)))
                end
                print(io_ctx.stdout, c.output)
                fake
            end

            # record the testset
            if VERSION < v"1.13.0-DEV.1037"
                testset.time_start = start
                testset.time_end = stop
            else
                #@atomic testset.time_start = start
                @atomic testset.time_end = stop
            end
            with_testset(testset) do
                Test.record(o_ts, testset)
            end
        end

        # mark remaining or running tests as interrupted
        for test in [tests; collect(keys(running_tests))]
            (test in completed_tests) && continue
            fake = Test.DefaultTestSet(test)
            c = IOCapture.capture() do
                Test.record(fake, Test.Error(:test_interrupted, test, nothing, Base.ExceptionStack([(exception = "skipped", backtrace = [])]), LineNumberNode(1)))
            end
            # don't print the output of interrupted tests, it's not useful
            with_testset(fake) do
                Test.record(o_ts, fake)
            end
        end
    end
    println(io_ctx.stdout)
    if VERSION >= v"1.13.0-DEV.1033"
        Test.print_test_results(io_ctx.stdout, o_ts, 1)
    else
        c = IOCapture.capture() do
            Test.print_test_results(o_ts, 1)
        end
        print(io_ctx.stdout, c.output)
    end
    if (VERSION >= v"1.13.0-DEV.1037" && !Test.anynonpass(o_ts)) ||
            (VERSION < v"1.13.0-DEV.1037" && !o_ts.anynonpass)
        println(io_ctx.stdout, "    \033[32;1mSUCCESS\033[0m")
    else
        println(io_ctx.stderr, "    \033[31;1mFAILURE\033[0m\n")
        if VERSION >= v"1.13.0-DEV.1033"
            Test.print_test_errors(io_ctx.stdout, o_ts)
        else
            c = IOCapture.capture() do
                Test.print_test_errors(o_ts)
            end
            print(io_ctx.stdout, c.output)
        end
        throw(Test.FallbackTestSetException("Test run finished with errors"))
    end
    return nothing
end # runtests

end # module ParallelTestRunner
