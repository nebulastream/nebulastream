
#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-tests
        "E2ECoordinatorSingleWorkerTest.cpp"
        "E2ECoordinatorMultiWorkerTest.cpp"
        "E2ECoordinatorMultiQueryTest.cpp"
        "E2EMonitoringTest.cpp"
        )
target_link_libraries(e2e-tests nes-core-test-util)
gtest_discover_tests(e2e-tests WORKING_DIRECTORY ${CMAKE_BINARY_DIR} PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR}  DISCOVERY_MODE PRE_TEST)
add_dependencies(e2e-tests nesCoordinator nesWorker)