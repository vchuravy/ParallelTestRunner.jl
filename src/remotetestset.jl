module RemoteTestSets

export RemoteTestSet, @remote_testset

import Test
import Test: AbstractTestSet, DefaultTestSet, Broken, Pass, Fail, Error, @testset

struct RemoteTestSet <: AbstractTestSet
    ts::DefaultTestSet

    RemoteTestSet(dts::DefaultTestSet) = new(dts)
end

RemoteTestSet(args...; kwargs...) = RemoteTestSet(DefaultTestSet(args...; kwargs...))

# Record testsets as usual
Test.record(ts::RemoteTestSet, t::Union{Broken, Pass, Fail, Error}; kwargs...) = Test.record(ts.ts, t; kwargs...)
Test.record(ts::RemoteTestSet, t::AbstractTestSet) = Test.record(ts.ts, t)

# This is the single method that needs changing
function Test.finish(ts::RemoteTestSet; print_results::Bool=Test.TESTSET_PRINT_ENABLE[])
    # This testset is just a placeholder,
    #   so it must be the top-most
    @assert Test.get_testset_depth() == 0

    # There should only ever be one child testset
    return only(ts.ts.results)
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
    testsettype = isnothing(testsettype) ? :(DefaultTestSet) : testsettype

    return esc(quote
        @testset RemoteTestSet "wrapper" begin
            @testset $testsettype $(otherargs...) $source
        end
    end)
end

end
