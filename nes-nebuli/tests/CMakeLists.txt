function(add_nes_nebuli_test)
    add_nes_test(${ARGN})
    target_link_libraries(${ARGV0} nes-nebuli)
    target_include_directories(${ARGV0} PRIVATE ..)
endfunction()


add_nes_nebuli_test(cost-test "CostTest.cpp")
