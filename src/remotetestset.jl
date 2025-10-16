module RemoteTestSets

export RemoteTestSet, @remote_testset

import Test
import Test: AbstractTestSet, DefaultTestSet, Broken, Pass, Fail, Error

struct RemoteTestSet <: AbstractTestSet
    ts::DefaultTestSet

    RemoteTestSet(dts::DefaultTestSet) = new(dts)
end

RemoteTestSet(args...; kwargs...) = RemoteTestSet(DefaultTestSet(args...; kwargs...))

function Base.propertynames(x::RemoteTestSet)
    (:ts, Base.propertynames(x.ts)...)
end
function Base.getproperty(ts::RemoteTestSet, sym::Symbol)
    if sym === :ts
        return Base.getfield(ts, :ts)
    end
    return Base.getfield(Base.getfield(ts, :ts), sym)
end
function Base.setproperty!(ts::RemoteTestSet, sym::Symbol, v)
    return Base.setproperty!(ts.ts, sym, v)
end

# Record testsets as usual
Test.record(ts::RemoteTestSet, t::Union{Broken, Pass, Fail, Error}; kwargs...) = Test.record(ts.ts, t; kwargs...)
Test.record(ts::RemoteTestSet, t::AbstractTestSet) = Test.record(ts.ts, t)

# This is the single method that needs changing
function Test.finish(ts::RemoteTestSet; print_results::Bool=Test.TESTSET_PRINT_ENABLE[])
    if Test.get_testset_depth() != 0
        throw(ErrorException("RemoteTestSet should only ever be a top-level TestSet"))
    end

    # Otherwise, just return the testset so it is returned from the @testset macro
    return only(ts.results)
end

Test.filter_errors(ts::RemoteTestSet) = Test.filter_errors(ts.ts)
Test.get_test_counts(ts::RemoteTestSet) = Test.get_test_counts(ts.ts)
Test.get_alignment(ts::RemoteTestSet, depth::Int) = Test.get_alignment(ts.ts, depth)

@static if isdefined(Test, :results) #VERSION > v"1.11.0-??"
    Test.results(ts::RemoteTestSet) = Test.results(ts.ts)
end
@static if isdefined(Test, :print_verbose) #VERSION > v"1.11.0-??"
    Test.print_verbose(ts::RemoteTestSet) = Test.print_verbose(ts.ts)
end
@static if isdefined(Test, :format_duration) #VERSION > v"1.?.0-"
    Test.format_duration(ts::RemoteTestSet) = Test.format_duration(ts.ts)
end
@static if isdefined(Test, :get_rng) #VERSION > v"1.12.0-"
    Test.get_rng(ts::RemoteTestSet) = Test.get_rng(ts.ts)
end
@static if isdefined(Test, :anynonpass) #VERSION > v"1.13.0-"
    Test.anynonpass(ts::RemoteTestSet) = Test.anynonpass(ts.ts)
end

macro remote_testset(args...)
    testsettype = nothing
    otherargs = []

    for arg in args[1:end-1]
        if isa(arg, Symbol) || Base.isexpr(arg, :.)
            testsettype = arg
        else
            push!(otherargs, arg)
        end
    end

    source = args[end]
    if isnothing(testsettype)
        testsettype = :(Test.DefaultTestSet)
    end

    # Build the inner @testset call
    inner_testset = Expr(:macrocall,
                         :(Test.var"@testset"),
                         LineNumberNode(@__LINE__, @__FILE__),
                         testsettype,
                         otherargs...,
                         source)

    # Build the outer @testset call with RemoteTestSet
    outer_testset = Expr(:macrocall,
                         :(Test.var"@testset"),
                         LineNumberNode(@__LINE__, @__FILE__),
                         :RemoteTestSet,
                         "wrapper",
                         Expr(:block, inner_testset))

    return esc(outer_testset)
end

end
