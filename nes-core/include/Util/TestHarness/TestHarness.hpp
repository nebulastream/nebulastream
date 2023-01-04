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

#ifndef NES_CORE_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_
#define NES_CORE_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_

#include <gtest/gtest.h>

#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestHarness/TestHarnessWorkerConfiguration.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>

#include <type_traits>
#include <utility>

/**
 * @brief This test harness wrap query deployment test in our test framework.
 */
namespace NES {

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
     * @param queryWithoutSink query string to test (without the sink operator)
     * @param restPort port for the rest service
     * @param rpcPort for for the grpc
     */
    explicit TestHarness(std::string queryWithoutSink,
                         uint16_t restPort,
                         uint16_t rpcPort,
                         std::filesystem::path testHarnessResourcePath,
                         uint64_t memSrcFrequency = 0,
                         uint64_t memSrcNumBuffToProcess = 1)
        : queryWithoutSink(std::move(queryWithoutSink)), coordinatorIPAddress("127.0.0.1"), restPort(restPort), rpcPort(rpcPort),
          memSrcFrequency(memSrcFrequency), memSrcNumBuffToProcess(memSrcNumBuffToProcess), bufferSize(4096),
          physicalSourceCount(0), topologyId(1), validationDone(false), topologySetupDone(false),
          testHarnessResourcePath(testHarnessResourcePath) {}

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

                if (harnessWorkerConfig->getSourceType() != TestHarnessWorkerConfiguration::MemorySource) {
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

    TestHarness& addLogicalSource(const std::string& logicalSourceName, const SchemaPtr& schema) {
        auto logicalSource = LogicalSource::create(logicalSourceName, schema);
        this->logicalSources.emplace_back(logicalSource);
        return *this;
    }

    /**
     * @brief check the schema size of the logical source and if it already exists
     * @param logical source name
     * @param schema schema of the source
     * @param physical source name
     */
    void checkAndAddLogicalSources() {

        for (const auto& logicalSource : logicalSources) {

            auto logicalSourceName = logicalSource->getLogicalSourceName();
            auto schema = logicalSource->getSchema();

            // Check if logical source already exists
            auto sourceCatalog = nesCoordinator->getSourceCatalog();
            if (!sourceCatalog->containsLogicalSource(logicalSourceName)) {
                NES_TRACE("TestHarness: logical source does not exist in the source catalog, adding a new logical source "
                          << logicalSourceName);
                sourceCatalog->addLogicalSource(logicalSourceName, schema);
            } else {
                // Check if it has the same schema
                if (!sourceCatalog->getSchemaForLogicalSource(logicalSourceName)->equals(schema, true)) {
                    NES_TRACE("TestHarness: logical source " << logicalSourceName
                                                             << " exists in the source catalog with "
                                                                "different schema, replacing it with a new schema");
                    sourceCatalog->removeLogicalSource(logicalSourceName);
                    sourceCatalog->addLogicalSource(logicalSourceName, schema);
                }
            }
        }
    }

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
                                               WorkerConfigurationPtr workerConfiguration = WorkerConfiguration::create()) {
        workerConfiguration->parentId = parentId;
#ifdef TFDEF
        workerConfiguration->isTfInstalled = true;
#endif// TFDEF
        std::string physicalSourceName = getNextPhysicalSourceName();
        auto workerId = getNextTopologyId();
        auto testHarnessWorkerConfiguration = TestHarnessWorkerConfiguration::create(workerConfiguration,
                                                                                     logicalSourceName,
                                                                                     physicalSourceName,
                                                                                     TestHarnessWorkerConfiguration::MemorySource,
                                                                                     workerId);
        testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
        return *this;
    }

    /**
     * @brief add a memory source to be used in the test
     * @param logical source name
     * @param schema schema of the source
     * @param physical source name
     */
    TestHarness& attachWorkerWithMemorySourceToCoordinator(const std::string& logicalSourceName) {
        //We are assuming coordinator will start with id 1
        return attachWorkerWithMemorySourceToWorkerWithId(std::move(logicalSourceName), 1);
    }

    /**
     * @brief add a memory source to be used in the test
     * @param logical source name
     * @param schema schema of the source
     * @param physical source name
     */
    TestHarness& attachWorkerWithLambdaSourceToCoordinator(const std::string& logicalSourceName,
                                                           PhysicalSourceTypePtr physicalSource,
                                                           WorkerConfigurationPtr workerConfiguration) {
        //We are assuming coordinator will start with id 1
        workerConfiguration->parentId = 1;
        std::string physicalSourceName = getNextPhysicalSourceName();
        auto workerId = getNextTopologyId();
        auto testHarnessWorkerConfiguration = TestHarnessWorkerConfiguration::create(workerConfiguration,
                                                                                     logicalSourceName,
                                                                                     physicalSourceName,
                                                                                     TestHarnessWorkerConfiguration::LambdaSource,
                                                                                     workerId);
        testHarnessWorkerConfiguration->setPhysicalSourceType(physicalSource);
        testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
        return *this;
    }

    /**
     * @brief add a csv source to be used in the test and connect to parent with specific parent id
     * @param logicalSourceName logical source name
     * @param csvSourceType csv source type
     * @param parentId id of the parent to connect
     */
    TestHarness& attachWorkerWithCSVSourceToWorkerWithId(const std::string& logicalSourceName,
                                                         const CSVSourceTypePtr& csvSourceType,
                                                         uint64_t parentId) {
        auto workerConfiguration = WorkerConfiguration::create();
        std::string physicalSourceName = getNextPhysicalSourceName();
        auto physicalSource = PhysicalSource::create(logicalSourceName, physicalSourceName, csvSourceType);
        workerConfiguration->physicalSources.add(physicalSource);
        workerConfiguration->parentId = parentId;
        uint32_t workerId = getNextTopologyId();
        auto testHarnessWorkerConfiguration = TestHarnessWorkerConfiguration::create(workerConfiguration,
                                                                                     logicalSourceName,
                                                                                     physicalSourceName,
                                                                                     TestHarnessWorkerConfiguration::CSVSource,
                                                                                     workerId);
        testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
        return *this;
    }

    /**
      * @brief add a csv source to be used in the test
      * @param logicalSourceName logical source name
      * @param csvSourceType csv source type
      */
    TestHarness& attachWorkerWithCSVSourceToCoordinator(const std::string& logicalSourceName,
                                                        const CSVSourceTypePtr& csvSourceType) {
        //We are assuming coordinator will start with id 1
        return attachWorkerWithCSVSourceToWorkerWithId(std::move(logicalSourceName), std::move(csvSourceType), 1);
    }

    /**
     * @brief add worker and connect to parent with specific parent id
     * @param parentId id of the Test Harness worker to connect
     * Note: The parent id can not be greater than the current testharness worker id
     */
    TestHarness& attachWorkerToWorkerWithId(uint32_t parentId) {

        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->parentId = parentId;
        uint32_t workerId = getNextTopologyId();
        auto testHarnessWorkerConfiguration = TestHarnessWorkerConfiguration::create(workerConfiguration, workerId);
        testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
        return *this;
    }

    /**
     * @brief add non source worker
     */
    TestHarness& attachWorkerToCoordinator() {
        //We are assuming coordinator will start with id 1
        return attachWorkerToWorkerWithId(1);
    }

    uint64_t getWorkerCount() { return testHarnessWorkerConfigurations.size(); }

    TestHarness& validate() {
        validationDone = true;
        if (this->logicalSources.empty()) {
            throw Exceptions::RuntimeException(
                "No Logical source defined. Please make sure you add logical source while defining up test harness.");
        }

        if (testHarnessWorkerConfigurations.empty()) {
            throw Exceptions::RuntimeException("TestHarness: No worker added to the test harness.");
        }

        uint64_t sourceCount = 0;
        for (const auto& workerConf : testHarnessWorkerConfigurations) {
            if (workerConf->getSourceType() == TestHarnessWorkerConfiguration::MemorySource && workerConf->getRecords().empty()) {
                throw Exceptions::RuntimeException("TestHarness: No Record defined for Memory Source with logical source Name: "
                                                   + workerConf->getLogicalSourceName()
                                                   + " and Physical source name : " + workerConf->getPhysicalSourceName()
                                                   + ". Please add data to the test harness.");
            }

            if (workerConf->getSourceType() == TestHarnessWorkerConfiguration::CSVSource
                || workerConf->getSourceType() == TestHarnessWorkerConfiguration::MemorySource
                || workerConf->getSourceType() == TestHarnessWorkerConfiguration::LambdaSource) {
                sourceCount++;
            }
        }

        if (sourceCount == 0) {
            throw Exceptions::RuntimeException("TestHarness: No Physical source defined in the test harness.");
        }
        return *this;
    }

    PhysicalSourcePtr createPhysicalSourceOfLambdaType(TestHarnessWorkerConfigurationPtr workerConf) {
        // create and populate memory source
        auto currentSourceNumOfRecords = workerConf->getRecords().size();

        auto logicalSourceName = workerConf->getLogicalSourceName();

        SchemaPtr schema;
        for (const auto& logicalSource : logicalSources) {
            if (logicalSource->getLogicalSourceName() == logicalSourceName) {
                schema = logicalSource->getSchema();
            }
        }

        if (!schema) {
            throw Exceptions::RuntimeException(
                "Unable to find logical source with name " + logicalSourceName
                + ". Make sure you are adding a logical source with the name to the test harness.");
        }

        return PhysicalSource::create(logicalSourceName,
                                      workerConf->getPhysicalSourceName(),
                                      workerConf->getPhysicalSourceType());
    };

    PhysicalSourcePtr createPhysicalSourceOfMemoryType(TestHarnessWorkerConfigurationPtr workerConf) {
        // create and populate memory source
        auto currentSourceNumOfRecords = workerConf->getRecords().size();

        auto logicalSourceName = workerConf->getLogicalSourceName();

        SchemaPtr schema;
        for (const auto& logicalSource : logicalSources) {
            if (logicalSource->getLogicalSourceName() == logicalSourceName) {
                schema = logicalSource->getSchema();
            }
        }

        if (!schema) {
            throw Exceptions::RuntimeException(
                "Unable to find logical source with name " + logicalSourceName
                + ". Make sure you are adding a logical source with the name to the test harness.");
        }

        auto tupleSize = schema->getSchemaSizeInBytes();
        NES_DEBUG("Tuple Size: " << tupleSize);
        NES_DEBUG("currentSourceNumOfRecords: " << currentSourceNumOfRecords);
        auto memAreaSize = currentSourceNumOfRecords * tupleSize;
        auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));

        auto currentRecords = workerConf->getRecords();
        for (std::size_t j = 0; j < currentSourceNumOfRecords; ++j) {
            memcpy(&memArea[tupleSize * j], currentRecords.at(j), tupleSize);
        }

        NES_ASSERT2_FMT(bufferSize % schema->getSchemaSizeInBytes() == 0,
                        "TestHarness: A record might span multiple buffers and this is not supported bufferSize="
                            << bufferSize << " recordSize=" << schema->getSchemaSizeInBytes());
        auto memorySourceType =
            MemorySourceType::create(memArea, memAreaSize, memSrcNumBuffToProcess, memSrcFrequency, "interval");
        return PhysicalSource::create(logicalSourceName, workerConf->getPhysicalSourceName(), memorySourceType);
    };

    /**
     * @brief Method to setup the topology
     * @param crdConfigFunctor A function pointer to specify the config changes of the CoordinatorConfiguration
     * @return the TestHarness
     */
    TestHarness& setupTopology(std::function<void(CoordinatorConfigurationPtr)> crdConfigFunctor =
                                   [](CoordinatorConfigurationPtr) {
                                   }) {
        if (!validationDone) {
            NES_THROW_RUNTIME_ERROR("Please call validate before calling setup.");
        }

        //Start Coordinator
        auto coordinatorConfiguration = CoordinatorConfiguration::create();
        coordinatorConfiguration->coordinatorIp = coordinatorIPAddress;
        coordinatorConfiguration->restPort = restPort;
        coordinatorConfiguration->rpcPort = rpcPort;
        crdConfigFunctor(coordinatorConfiguration);

        nesCoordinator = std::make_shared<NesCoordinator>(coordinatorConfiguration);
        auto coordinatorRPCPort = nesCoordinator->startCoordinator(/**blocking**/ false);
        //Add all logical sources
        checkAndAddLogicalSources();

        for (auto& workerConf : testHarnessWorkerConfigurations) {

            //Fetch the worker configuration
            auto workerConfiguration = workerConf->getWorkerConfiguration();

            //Set ports at runtime
            workerConfiguration->coordinatorPort = coordinatorRPCPort;
            workerConfiguration->coordinatorIp = coordinatorIPAddress;

            switch (workerConf->getSourceType()) {
                case TestHarnessWorkerConfiguration::MemorySource: {
                    auto physicalSource = createPhysicalSourceOfMemoryType(workerConf);
                    workerConfiguration->physicalSources.add(physicalSource);
                    break;
                }
                case TestHarnessWorkerConfiguration::LambdaSource: {
                    auto physicalSource = createPhysicalSourceOfLambdaType(workerConf);
                    workerConfiguration->physicalSources.add(physicalSource);
                    break;
                }
                default: break;
            }

            NesWorkerPtr nesWorker = std::make_shared<NesWorker>(std::move(workerConfiguration));
            nesWorker->start(/**blocking**/ false, /**withConnect**/ true);

            //We are assuming that coordinator has a node id 1
            nesWorker->replaceParent(1, nesWorker->getWorkerConfiguration()->parentId.getValue());

            //Add Nes Worker to the configuration.
            //Note: this is required to stop the NesWorker at the end of the test
            workerConf->setQueryStatusListener(nesWorker);
        }

        topologySetupDone = true;
        return *this;
    }

    /**
     * @brief execute the test based on the given operator, pushed elements, and number of workers,
     * then return the result of the query execution
     * @param placementStrategyName: placement strategy name
     * @param faultTolerance: chosen fault tolerance guarantee
     * @param lineage: chosen lineage type
     * @param numberOfContentToExpect: total number of tuple expected in the query result
     * @return output string
     */
    template<typename T>
    std::vector<T> getOutput(uint64_t numberOfContentToExpect,
                             std::string placementStrategyName,
                             std::string faultTolerance,
                             std::string lineage,
                             uint64_t testTimeout = 60) {

        if (!topologySetupDone || !validationDone) {
            throw Exceptions::RuntimeException(
                "Make sure to call first validate() and then setupTopology() to the test harness before checking the output");
        }

        QueryServicePtr queryService = nesCoordinator->getQueryService();
        QueryCatalogServicePtr queryCatalogService = nesCoordinator->getQueryCatalogService();

        // local fs
        std::string filePath = testHarnessResourcePath / "testHarness.out";
        remove(filePath.c_str());

        //register query
        std::string queryString =
            queryWithoutSink + R"(.sink(FileSinkDescriptor::create(")" + filePath + R"(" , "NES_FORMAT", "APPEND"));)";
        auto faultToleranceMode = FaultToleranceType::getFromString(faultTolerance);

        auto lineageMode = LineageType::getFromString(lineage);

        QueryId queryId = queryService->validateAndQueueAddQueryRequest(queryString,
                                                                        std::move(placementStrategyName),
                                                                        faultToleranceMode,
                                                                        lineageMode);

        if (!TestUtils::waitForQueryToStart(queryId, queryCatalogService)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
        }

        // Check if the size of output struct match with the size of output schema
        // Output struct might be padded, in this case the size is not equal to the total size of its field
        // Currently, we need to produce a result with the schema that does not cause the associated struct to be padded
        // (e.g., the size is multiple of 8)
        uint64_t outputSchemaSizeInBytes = queryCatalogService->getEntryForQuery(queryId)
                                               ->getExecutedQueryPlan()
                                               ->getSinkOperators()[0]
                                               ->getOutputSchema()
                                               ->getSchemaSizeInBytes();
        NES_DEBUG("TestHarness: outputSchema: " << queryCatalogService->getEntryForQuery(queryId)
                                                       ->getInputQueryPlan()
                                                       ->getSinkOperators()[0]
                                                       ->getOutputSchema()
                                                       ->toString());
        NES_ASSERT(outputSchemaSizeInBytes == sizeof(T),
                   "The size of output struct does not match output schema."
                   " Output struct:"
                       << std::to_string(sizeof(T)) << " Schema:" << std::to_string(outputSchemaSizeInBytes));

        if (!TestUtils::checkBinaryOutputContentLengthOrTimeout<T>(queryId,
                                                                   queryCatalogService,
                                                                   numberOfContentToExpect,
                                                                   filePath,
                                                                   testTimeout)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkBinaryOutputContentLengthOrTimeout returns false, "
                                    "number of buffers to expect="
                                    << std::to_string(numberOfContentToExpect));
        }

        if (!TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkStoppedOrTimeout returns false for query with id= " << queryId);
        }

        std::ifstream ifs(filePath.c_str());
        if (!ifs.good()) {
            NES_WARNING("TestHarness:ifs.good() returns false for query with id " << queryId << " file path=" << filePath);
        }

        // check the length of the output file
        ifs.seekg(0, std::ifstream::end);
        auto length = ifs.tellg();
        ifs.seekg(0, std::ifstream::beg);

        // read the binary output as a vector of T
        std::vector<T> outputVector;
        outputVector.resize(length / sizeof(T));
        auto* buff = reinterpret_cast<char*>(outputVector.data());
        ifs.read(buff, length);

        NES_DEBUG("TestHarness: ExecutedQueryPlan: "
                  << queryCatalogService->getEntryForQuery(queryId)->getExecutedQueryPlan()->toString());
        queryPlan = queryCatalogService->getEntryForQuery(queryId)->getExecutedQueryPlan();

        for (const auto& worker : testHarnessWorkerConfigurations) {
            worker->getNesWorker()->stop(false);
        }
        nesCoordinator->stopCoordinator(false);

        return outputVector;
    }

    TopologyPtr getTopology() {

        if (!validationDone && !topologySetupDone) {
            throw Exceptions::RuntimeException(
                "Make sure to call first validate() and then setupTopology() to the test harness before checking the output");
        }
        return nesCoordinator->getTopology();
    };

    const QueryPlanPtr& getQueryPlan() const { return queryPlan; }

  private:
    std::string getNextPhysicalSourceName() {
        physicalSourceCount++;
        return std::to_string(physicalSourceCount);
    }

    uint32_t getNextTopologyId() {
        topologyId++;
        return topologyId;
    }

    std::string queryWithoutSink;
    std::string coordinatorIPAddress;
    uint16_t restPort;
    uint16_t rpcPort;
    uint64_t memSrcFrequency;
    uint64_t memSrcNumBuffToProcess;
    uint64_t bufferSize;
    NesCoordinatorPtr nesCoordinator;
    std::vector<LogicalSourcePtr> logicalSources;
    std::vector<TestHarnessWorkerConfigurationPtr> testHarnessWorkerConfigurations;
    uint32_t physicalSourceCount;
    uint32_t topologyId;
    bool validationDone;
    bool topologySetupDone;
    std::filesystem::path testHarnessResourcePath;
    QueryPlanPtr queryPlan;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_
