add_executable(spike-cpp23 main.cpp)
set_target_properties(spike-cpp23 PROPERTIES
    SUFFIX ".cjs"
    LINK_FLAGS "-sENVIRONMENT=node"
)
