@test true

foo() = @warn "This is a warning"
@test_warn "This is a warning" foo()
