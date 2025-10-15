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

@testset "custom testrecord" begin
    custom_record_init = quote
        struct CustomTestRecord <: ParallelTestRunner.AbstractTestRecord
            # TODO: Would it be better to wrap "ParallelTestRunner.TestRecord "
            value::Any          # AbstractTestSet or TestSetException
            output::String      # captured stdout/stderr

            # stats
            time::Float64
            bytes::UInt64
            gctime::Float64
            rss::UInt64
        end
        function ParallelTestRunner.memory_usage(rec::CustomTestRecord)
            return rec.rss
        end
        function ParallelTestRunner.test_IOContext(::Type{CustomTestRecord}, args...)
            return ParallelTestRunner.test_IOContext(ParallelTestRunner.TestRecord, args...)
        end
        function ParallelTestRunner.runtest(::Type{CustomTestRecord}, f, name, init_code, color, (; say_hello))
            function inner()
                # generate a temporary module to execute the tests in
                mod = @eval(Main, module $(gensym(name)) end)
                @eval(mod, import ParallelTestRunner: Test, Random)
                @eval(mod, using .Test, .Random)

                Core.eval(mod, init_code)

                data = @eval mod begin
                    GC.gc(true)
                    Random.seed!(1)

                    mktemp() do path, io
                        stats = redirect_stdio(stdout=io, stderr=io) do
                            @timed try
                                if say_hello
                                    println("Hello from test '$name'")
                                end
                                @testset $name begin
                                    $f
                                end
                            catch err
                                isa(err, Test.TestSetException) || rethrow()

                                # return the error to package it into a TestRecord
                                err
                            end
                        end
                        close(io)
                        output = read(path, String)
                        (; testset=stats.value, output, stats.time, stats.bytes, stats.gctime)

                    end
                end

                # process results
                rss = Sys.maxrss()
                record = TestRecord(data..., rss)

                GC.gc(true)
                return record
            end

            @static if VERSION >= v"1.13.0-DEV.1044"
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
        end
    end # quote
    eval(custom_record_init)

    io = IOBuffer()

    runtests(ParallelTestRunner, ["--verbose"]; custom_record_init, RecordType=CustomTestRecord, custom_args=(; say_hello=true), stdout=io, stderr=io)
    str = String(take!(io))

    @test contains(str, r"basic .+ started at")
    @test contains(str, r"Hello from test 'basic'")
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
    @test contains(str, "ProcessExitedException")
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
