option(USE_LIBFUZZER "" ON)

if (USE_LIBFUZZER)
    add_compile_options(-fsanitize=fuzzer-no-link)
    add_link_options(-fsanitize=fuzzer-no-link)
endif ()