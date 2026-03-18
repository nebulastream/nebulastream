add_executable(spike-embind main.cpp)
target_link_options(spike-embind PRIVATE -lembind)
set_target_properties(spike-embind PROPERTIES
    SUFFIX ".cjs"
    LINK_FLAGS "-sMODULARIZE=1 -sEXPORT_ES6=0 -sENVIRONMENT=node"
)
