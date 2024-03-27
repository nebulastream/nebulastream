
#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_nes_integration_test(e2e-tests
        "E2ECoordinatorSingleWorkerTest.cpp"
        "E2ECoordinatorMultiWorkerTest.cpp"
        "E2ECoordinatorMultiQueryTest.cpp"
        "E2EMonitoringTest.cpp"
        )