add_subdirectory(pass)

add_subdirectory(example)

add_dependencies(example MinimalPass)

target_compile_options(example PRIVATE
        "-fpass-plugin=$<TARGET_FILE:MinimalPass>"
)

