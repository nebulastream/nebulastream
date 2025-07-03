add_executable(nes-nebusqli-fuzz NebuSQLiFuzz.cpp)
target_link_libraries(nes-nebusqli-fuzz PUBLIC nes-nebuli-lib argparse::argparse nes-sql-parser)
target_compile_options(nes-nebusqli-fuzz PRIVATE -fsanitize=fuzzer)
target_link_options(nes-nebusqli-fuzz PRIVATE -fsanitize=fuzzer)