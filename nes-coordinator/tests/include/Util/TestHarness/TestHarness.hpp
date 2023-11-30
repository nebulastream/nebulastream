/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_

#include <API/AttributeField.hpp>
#include <API/Query.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/BufferManager.hpp>
#include <Services/QueryService.hpp>
#include <Util/Core.hpp>
#include <Util/TestHarness/TestHarnessWorkerConfiguration.hpp>
#include <Util/TestUtils.hpp>
#include <cstring>
#include <filesystem>
#include <type_traits>
#include <utility>

/**
 * @brief This test harness wrap query deployment test in our test framework.
 */
namespace NES {

class CSVSourceType;
using CSVSourceTypePtr = std::shared_ptr<CSVSourceType>;

/// Create compile-time tests that allow checking a specific function's type for a specific function by calling
///     [function-name]CompilesFromType<Return Type, Types of arguments, ...>
/// or  [function-name]Compiles<Return Type, argument 1, argument 2>.
///
/// Note that the non-type compile time checks for the non-type template arguments are limited to consteval-
/// constructible and non-floating point types.
/// Another limitation is that as of now, type and non type template argument tests cannot be mixed.
#define SETUP_COMPILE_TIME_TESTS(name, f)                                                                                        \
    SETUP_COMPILE_TIME_TEST(name, f);                                                                                            \
    SETUP_COMPILE_TIME_TEST_ARGS(name, f)

/// Check if function #func compiles from constexpr arguments and produce the expected return type that is provided
/// as the check's first template argument.
#define SETUP_COMPILE_TIME_TEST_ARGS(name, func)                                                                                 \
    namespace detail {                                                                                                           \
    template<typename, auto...>                                                                                                  \
    struct name##FromArgs : std::false_type {};                                                                                  \
    template<auto... args>                                                                                                       \
    struct name##FromArgs<decltype(func(args...)), args...> : std::true_type {};                                                 \
    }                                                                                                                            \
    template<typename R, auto... a>                                                                                              \
    using name##Compiles = detail::name##FromArgs<R, a...>

/// Check if function #func compiles from argument of given types produce the expected return type that is provided as
/// the check's first template argument.
#define SETUP_COMPILE_TIME_TEST(name, func)                                                                                      \
    namespace detail {                                                                                                           \
    template<typename, typename...>                                                                                              \
    struct name##FromType : std::false_type {};                                                                                  \
    template<typename... Ts>                                                                                                     \
    struct name##FromType<decltype(func(std::declval<Ts>()...)), Ts...> : std::true_type {};                                     \
    }                                                                                                                            \
    template<typename... Args>                                                                                                   \
    using name##CompilesFromType = detail::name##FromType<Args...>

class TestHarness {
  public:
    /**
     * @brief The constructor of TestHarness
     * @param numWorkers number of worker (each for one physical source) to be used in the test
     * @param queryWithoutSink query object to test (without the sink operator)
     * @param restPort port for the rest service
     * @param rpcPort for for the grpc
     */
    explicit TestHarness(Query queryWithoutSink,
                         uint16_t restPort,
                         uint16_t rpcPort,
                         std::filesystem::path testHarnessResourcePath,
                         uint64_t memSrcFrequency = 0,
                         uint64_t memSrcNumBuffToProcess = 1);

    /**
     * @brief Enable new request executor
     * @return self
     */
    TestHarness& enableNewRequestExecutor();

    /**
     * @brief Sets the join strategy
     * @param joinStrategy
     * @return Self
     */
    TestHarness& setJoinStrategy(QueryCompilation::StreamJoinStrategy& newJoinStrategy);

    /**
     * @brief Sets the join strategy
     * @param joinStrategy
     * @return Self
     */
    TestHarness& setWindowingStrategy(QueryCompilation::WindowingStrategy& newWindowingStrategy);

    /**
         * @brief push a single element/tuple to specific source
         * @param element element of Record to push
         * @param workerId id of the worker whose source will produce the pushed element
         */
    template<typename T>
    TestHarness& pushElement(T element, uint64_t workerId) {
        if (workerId > topologyId) {
            NES_THROW_RUNTIME_ERROR("TestHarness: workerId " + std::to_string(workerId) + " does not exists");
        }

        bool found = false;
        for (const auto& harnessWorkerConfig : testHarnessWorkerConfigurations) {
            if (harnessWorkerConfig->getWorkerId() == workerId) {
                found = true;
                if (!std::is_class<T>::value) {
                    NES_THROW_RUNTIME_ERROR("TestHarness: tuples must be instances of struct");
                }

                if (harnessWorkerConfig->getSourceType()
                    != TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::MemorySource) {
                    NES_THROW_RUNTIME_ERROR("TestHarness: Record can be pushed only for source of Memory type.");
                }

                SchemaPtr schema;
                for (const auto& logicalSource : logicalSources) {
                    if (logicalSource->getLogicalSourceName() == harnessWorkerConfig->getLogicalSourceName()) {
                        schema = logicalSource->getSchema();
                        break;
                    }
                }

                if (!schema) {
                    NES_THROW_RUNTIME_ERROR("TestHarness: Unable to find schema for logical source "
                                            + harnessWorkerConfig->getLogicalSourceName()
                                            + ". Make sure you have defined a logical source with this name in test harness");
                }

                if (sizeof(T) != schema->getSchemaSizeInBytes()) {
                    NES_THROW_RUNTIME_ERROR("TestHarness: tuple size and schema size does not match");
                }

                auto* memArea = reinterpret_cast<uint8_t*>(malloc(sizeof(T)));
                memcpy(memArea, reinterpret_cast<uint8_t*>(&element), sizeof(T));
                harnessWorkerConfig->addRecord(memArea);
                break;
            }
        }

        if (!found) {
            NES_THROW_RUNTIME_ERROR("TestHarness: Unable to locate worker with id " + std::to_string(workerId));
        }

        return *this;
    }

    TestHarness& addLogicalSource(const std::string& logicalSourceName, const SchemaPtr& schema);

    /**
     * @brief check the schema size of the logical source and if it already exists
     * @param logical source name
     * @param schema schema of the source
     * @param physical source name
     */
    void checkAndAddLogicalSources();

    /**
     * @brief add a memory source to be used in the test and connect to parent with specific parent id
     * @param logical source name
     * @param schema schema of the source
     * @param physical source name
     * @param parentId id of the parent to connect
     */
    TestHarness&
    attachWorkerWithMemorySourceToWorkerWithId(const std::string& logicalSourceName,
                                               uint32_t parentId,
                                               WorkerConfigurationPtr workerConfiguration = WorkerConfiguration::create());

    /**
     * @brief add a memory source to be used in the test
     * @param logical source name
     * @param schema schema of the source
     * @param physical source name
     */
    TestHarness& attachWorkerWithMemorySourceToCoordinator(const std::string& logicalSourceName);

    /**
     * @brief add a memory source to be used in the test
     * @param physicalSourceType schema of the source
     * @param workerConfiguration source name
     */
    TestHarness& attachWorkerWithLambdaSourceToCoordinator(PhysicalSourceTypePtr physicalSourceType,
                                                           WorkerConfigurationPtr workerConfiguration);

    /**
     * @brief add a csv source to be used in the test and connect to parent with specific parent id
     * @param logicalSourceName logical source name
     * @param csvSourceType csv source type
     * @param parentId id of the parent to connect
     */
    TestHarness& attachWorkerWithCSVSourceToWorkerWithId(const CSVSourceTypePtr& csvSourceType, uint64_t parentId);

    /**
      * @brief add a csv source to be used in the test
      * @param logicalSourceName logical source name
      * @param csvSourceType csv source type
      */
    TestHarness& attachWorkerWithCSVSourceToCoordinator(const CSVSourceTypePtr& csvSourceType);

    /**
     * @brief add worker and connect to parent with specific parent id
     * @param parentId id of the Test Harness worker to connect
     * Note: The parent id can not be greater than the current testharness worker id
     */
    TestHarness& attachWorkerToWorkerWithId(uint32_t parentId);

    /**
     * @brief add non source worker
     */
    TestHarness& attachWorkerToCoordinator();

    uint64_t getWorkerCount();

    TestHarness& validate();

    PhysicalSourceTypePtr createPhysicalSourceOfLambdaType(TestHarnessWorkerConfigurationPtr workerConf);

    PhysicalSourceTypePtr createPhysicalSourceOfMemoryType(TestHarnessWorkerConfigurationPtr workerConf);

    /**
     * @brief Method to setup the topology
     * @param crdConfigFunctor A function pointer to specify the config changes of the CoordinatorConfiguration
     * @return the TestHarness
     */
    TestHarness& setupTopology(std::function<void(CoordinatorConfigurationPtr)> crdConfigFunctor =
                                   [](CoordinatorConfigurationPtr) {
                                   });

    /**
     * @brief Runs the query based on the given operator, pushed elements, and number of workers.
     * @param numberOfBytesToExpect
     * @param placementStrategyName: placement strategy name
     * @param testTimeoutInSeconds
     * @return TestHarness
     */
    TestHarness& runQuery(uint64_t numberOfBytesToExpect,
                          const std::string& placementStrategyName = "BottomUp",
                          uint64_t testTimeoutInSeconds = 60);

    /**
     * @brief Returns the output for the previously run query. Support also data types with variable data size
     * @return Vector of DynamicTupleBuffers
     */
    std::vector<Runtime::MemoryLayouts::DynamicTupleBuffer> getOutput();

    /**
     * @brief Returns the output schema of the query
     * @return SchemaPtr
     */
    SchemaPtr getOutputSchema();

    TopologyPtr getTopology();
    const QueryPlanPtr& getQueryPlan() const;

    Runtime::BufferManagerPtr getBufferManager() const;

  private:
    std::string getNextPhysicalSourceName();
    uint32_t getNextTopologyId();

    const std::chrono::seconds SETUP_TIMEOUT_IN_SEC = std::chrono::seconds(2);
    const QueryPtr queryWithoutSink;
    std::string coordinatorIPAddress;
    uint16_t restPort;
    uint16_t rpcPort;
    bool useNewRequestExecutor;
    uint64_t memSrcFrequency;
    uint64_t memSrcNumBuffToProcess;
    uint64_t bufferSize;
    NesCoordinatorPtr nesCoordinator;
    std::vector<LogicalSourcePtr> logicalSources;
    std::vector<TestHarnessWorkerConfigurationPtr> testHarnessWorkerConfigurations;
    uint32_t physicalSourceCount;
    uint32_t topologyId;
    QueryCompilation::StreamJoinStrategy joinStrategy;
    QueryCompilation::WindowingStrategy windowingStrategy;
    bool validationDone;
    bool topologySetupDone;
    std::filesystem::path filePath;
    QueryPlanPtr queryPlan;
    QueryId queryId;
    Runtime::BufferManagerPtr bufferManager;
};
}// namespace NES

#endif  // NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_
