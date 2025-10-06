function somefunc()
    return true
end

nothing # File is loaded via a remotecall to "include". Ensure it returns "nothing".
