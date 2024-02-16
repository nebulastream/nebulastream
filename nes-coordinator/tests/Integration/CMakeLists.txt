
#-------------------------------##############----------------------------------
#------------------------------- # Integration Test Suite --------------------------------

### NES Filter Push Down Tests ###
add_nes_integration_test(filter-push-down-integration-tests "Optimizer/FilterPushDownTest.cpp")

### NES Connectivity Controller Tests ###
add_nes_integration_test(connectivity-controller-integration-tests "REST/Controller/ConnectivityControllerTest.cpp")

### NES Query Catalog Controller Test ###
add_nes_integration_test(query-catalog-controller-integration-tests "REST/Controller/QueryCatalogControllerTest.cpp")

### NES Monitoring Controller Test ###
add_nes_integration_test(monitoring-controller-tests "REST/Controller/MonitoringControllerTest.cpp")

### NES Topology Controller Test ###
add_nes_integration_test(topology-controller-integration-tests "REST/Controller/TopologyControllerTest.cpp")

### NES Udf Catalog Controller Test ###
add_nes_integration_test(udf-catalog-controller-integration-tests "REST/Controller/UDFCatalogControllerTest.cpp")

### NES Query Controller Test ###
add_nes_integration_test(query-controller-integration-tests "REST/Controller/QueryControllerTest.cpp")

### NES Source Catalog Controller Test ###
add_nes_integration_test(source-catalog-controller-integration-tests "REST/Controller/SourceCatalogControllerTest.cpp")

### NES Location Controller Tests ###
add_nes_integration_test(location-controller-integration-tests "REST/Controller/LocationControllerIntegrationTest.cpp")


### Filter Push Down Test ###
#add_nes_integration_test(filter-push-down-test "Optimizer/FilterPushDownTest.cpp")

#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_nes_integration_test(multi-worker-test "MultiWorkerTest.cpp")


### QueryDeploymentTest ###
add_nes_integration_test(query-deployment-test "QueryDeploymentTest.cpp")


### MQTTSinkDepleymentTest ###
add_nes_integration_test(mqtt-sink-deployment-test "MQTTSinkDeploymentTest.cpp")


### RenameTest ###
add_nes_integration_test(rename-test "RenameTest.cpp")


### JoinDeploymentTest ###
add_nes_integration_test(join-deployment-test "JoinDeploymentTest.cpp")

### windowDeploymentTest ###
add_nes_integration_test(window-deployment-test "Windowing/WindowDeploymentTest.cpp")
add_nes_integration_test(ysb-deployment-test "Windowing/YSBDeploymentTest.cpp")
add_nes_integration_test(ktm-deployment-test "Windowing/KTMDeploymentTest.cpp")


### window integration tests ###
add_nes_integration_test(keyed-tumbling-window-tests "Windowing/TumblingWindow/KeyedTumblingWindowTests.cpp")
add_nes_integration_test(non-keyed-tumbling-window-tests "Windowing/TumblingWindow/NotKeyedlTumblingWindowTests.cpp")
add_nes_integration_test(keyed-sliding-window-tests "Windowing/SlidingWindow/KeyedSlidingWindowTests.cpp")
add_nes_integration_test(non-keyed-sliding-window-tests "Windowing/SlidingWindow/NonKeyedSlidingWindowTests.cpp")


### MultipleWindowsTest ###
add_nes_integration_test(multi-window-test "Windowing/MultipleWindowsTest.cpp")


### MultiJoinsTest ###
add_nes_integration_test(multi-join-test "MultipleJoinsTest.cpp")


### MultiThreadedTest ###
if (NES_BUILD_MULTITHREADED)
add_nes_integration_test(multi-thread-test "MultiThreadedTest.cpp" "JoinMultiThreadedTest.cpp")
endif ()

### unionDeploymentTest ###
add_nes_integration_test(union-deployment-test "UnionDeploymentTest.cpp")

### DeepHierarchyTopologyTestDeepHierarchyTopologyTest ###
add_nes_integration_test(deep-hierarchy-test "DeepHierarchyTopologyTest.cpp")

### WorkerCoordinatorStarter Tests ###
add_nes_integration_test(worker-coordinator-starter-tests "WorkerCoordinatorStarterTest.cpp")

### WorkerCoordinatorStarter Tests ###
add_nes_integration_test(update-topology-remote-test "UpdateTopologyRemoteTest.cpp")

# NetworkStackIntegrationTest
add_nes_integration_test(network-stack-integration-test "NetworkStackIntegrationTest.cpp")

# DistributedMatrixJoinIntegrationTest
add_nes_integration_test(nemo-integration-test "DistributedMatrixJoinIntegrationTest.cpp")

###  Source Integration tests ###
add_nes_integration_test(source-integration-tests
        "Source/MemorySourceIntegrationTest.cpp"
        "Source/VariableLengthIntegrationTest.cpp"
        "Source/BenchmarkSourceIntegrationTest.cpp"
        "Source/SourceCatalogRemoteTest.cpp"
        "Source/TCPSourceIntegrationTest.cpp"
        "Source/ContinuousSourceTest.cpp"
        "Source/LambdaSourceIntegrationTest.cpp")
add_dependencies(source-integration-tests nesCoordinator nesWorker)


### TestHarnessUtilTest ###
add_nes_integration_test(test-harness-util-test "TestHarnessUtilTest.cpp")

### Query Failure Test
add_nes_integration_test(query-failure-test "QueryFailureTest.cpp")

### Query Redeployment Integration Test ###
add_nes_integration_test(query-redeployment-integration-tests "QueryRedeploymentIntegrationTest.cpp")

### Explain Integration Test ###
add_nes_integration_test(explain-request-integration-test "ExplainRequestIntegrationTest.cpp")

### Location Integration Test ###
# Enabled with issue #4270
#add_nes_integration_test(location-integration-tests "LocationIntegrationTests.cpp")
#target_link_libraries(location-integration-tests nes-coordinator-test-util)