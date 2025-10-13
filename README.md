# ParallelTestRunner.jl

Simple parallel test runner for Julia tests with autodiscovery.

## Usage

```julia
julia> import Pkg

julia> Pkg.test("ParallelTestRunner"; test_args=["--help"])

Usage: runtests.jl [--help] [--list] [--jobs=N] [TESTS...]

   --help             Show this text.
   --list             List all available tests.
   --verbose          Print more information during testing.
   --quickfail        Fail the entire run as soon as a single test errored.
   --jobs=N           Launch `N` processes to perform tests (default: Sys.CPU_THREADS).

   Remaining arguments filter the tests that will be executed.
```

## Setup

`ParallelTestRunner` runs each file inside your `test/` concurrently and isolated.
First you should remove all `include` statements that you added.

Then in your `test/runtests.jl` add:

```julia
using ParallelTestRunner

runtests(MyModule, ARGS)
```

### Filtering

`runtests` takes a keyword argument that acts as a filter function

```julia
function testfilter(test)
    if Sys.iswindows() && test == "ext/specialfunctions"
        return false
    end
    return true
end

runtests(MyModule, ARGS; testfilter)
```

### Provide defaults

`runtests` takes a keyword argument that one can use to provide default definitions to be loaded before each testfile.
As an example one could always load `Test` and the package under test.

```julia
const init_code = quote
   using Test
   using MyPackage
end

runtests(MyModule, ARGS; init_code)
```

## Packages using ParallelTestRunner.jl

There are a few packages already using `ParallelTestRunner.jl` to parallelize their tests, you can look at their setups if you need inspiration to move your packages as well:

* [`Enzyme.jl`](https://github.com/EnzymeAD/Enzyme.jl/blob/main/test/runtests.jl)
* [`GPUCompiler.jl`](https://github.com/JuliaGPU/GPUCompiler.jl/blob/master/test/runtests.jl)

## Inspiration
Based on [@maleadt](https://github.com/maleadt) test infrastructure for [CUDA.jl](https://github.com/JuliaGPU/CUDA.jl).
