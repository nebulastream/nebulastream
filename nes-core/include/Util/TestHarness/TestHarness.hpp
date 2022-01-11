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

#ifndef NES_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_
#define NES_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestHarness/TestHarnessWorkerConfiguration.hpp>
#include <Util/TestUtils.hpp>
#include <type_traits>
#include <utility>
#include <filesystem>

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
          physicalSourceCount(0), topologyId(1), validationDone(false), topologySetupDone(false), testHarnessResourcePath(testHarnessResourcePath) {}

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
     * @brief check the schema size of the logical stream and if it already exists
     * @param logical stream name
     * @param schema schema of the source
     * @param physical stream name
     */
    void checkAndAddLogicalSources() {

        for (const auto& logicalSource : logicalSources) {

            auto logicalSourceName = logicalSource->getLogicalSourceName();
            auto schema = logicalSource->getSchema();

            // Check if logical stream already exists
            auto streamCatalog = nesCoordinator->getStreamCatalog();
            if (!streamCatalog->containsLogicalSource(logicalSourceName)) {
                NES_TRACE("TestHarness: logical source does not exist in the stream catalog, adding a new logical stream "
                          << logicalSourceName);
                streamCatalog->addLogicalStream(logicalSourceName, schema);
            } else {
                // Check if it has the same schema
                if (!streamCatalog->getSchemaForLogicalStream(logicalSourceName)->equals(schema, true)) {
                    NES_TRACE("TestHarness: logical source " << logicalSourceName
                                                             << " exists in the stream catalog with "
                                                                "different schema, replacing it with a new schema");
                    streamCatalog->removeLogicalStream(logicalSourceName);
                    streamCatalog->addLogicalStream(logicalSourceName, schema);
                }
            }
        }
    }

    /**
     * @brief add a memory source to be used in the test and connect to parent with specific parent id
     * @param logical stream name
     * @param schema schema of the source
     * @param physical stream name
     * @param parentId id of the parent to connect
     */
    TestHarness& attachWorkerWithMemorySourceToWorkerWithId(const std::string& logicalStreamName, uint32_t parentId) {

        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->setParentId(parentId);
        std::string physicalSourceName = getNextPhysicalSourceName();
        auto workerId = getNextTopologyId();
        auto testHarnessWorkerConfiguration = TestHarnessWorkerConfiguration::create(workerConfiguration,
                                                                                     logicalStreamName,
                                                                                     physicalSourceName,
                                                                                     TestHarnessWorkerConfiguration::MemorySource,
                                                                                     workerId);
        testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
        return *this;
    }

    /**
     * @brief add a memory source to be used in the test
     * @param logical stream name
     * @param schema schema of the source
     * @param physical stream name
     */
    TestHarness& attachWorkerWithMemorySourceToCoordinator(const std::string& logicalStreamName) {
        //We are assuming coordinator will start with id 1
        return attachWorkerWithMemorySourceToWorkerWithId(std::move(logicalStreamName), 1);
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
        workerConfiguration->addPhysicalSource(physicalSource);
        workerConfiguration->setParentId(parentId);
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
        workerConfiguration->setParentId(parentId);
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
            throw Exception("No Logical source defined. Please make sure you add logical source while defining up test harness.");
        }

        if (testHarnessWorkerConfigurations.empty()) {
            throw Exception("TestHarness: No worker added to the test harness.");
        }

        uint64_t sourceCount = 0;
        for (const auto& workerConf : testHarnessWorkerConfigurations) {
            if (workerConf->getSourceType() == TestHarnessWorkerConfiguration::MemorySource && workerConf->getRecords().empty()) {
                throw Exception("TestHarness: No Record defined for Memory Source with logical source Name: "
                                + workerConf->getLogicalSourceName() + " and Physical source name : "
                                + workerConf->getPhysicalSourceName() + ". Please add data to the test harness.");
            }

            if (workerConf->getSourceType() == TestHarnessWorkerConfiguration::CSVSource
                || workerConf->getSourceType() == TestHarnessWorkerConfiguration::MemorySource) {
                sourceCount++;
            }
        }

        if (sourceCount == 0) {
            throw Exception("TestHarness: No Physical source defined in the test harness.");
        }
        return *this;
    }

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
            throw Exception("Unable to find logical source with name " + logicalSourceName
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
            MemorySourceType::create(memArea, memAreaSize, memSrcNumBuffToProcess, memSrcFrequency, "frequency");
        return PhysicalSource::create(logicalSourceName, workerConf->getPhysicalSourceName(), memorySourceType);
    };

    TestHarness& setupTopology() {

        if (!validationDone) {
            NES_THROW_RUNTIME_ERROR("Please call validate before calling setup.");
        }

        //Start Coordinator
        auto coordinatorConfiguration = CoordinatorConfiguration::create();
        coordinatorConfiguration->setCoordinatorIp(coordinatorIPAddress);
        coordinatorConfiguration->setRestPort(restPort);
        coordinatorConfiguration->setRpcPort(rpcPort);
        nesCoordinator = std::make_shared<NesCoordinator>(coordinatorConfiguration);
        auto coordinatorRPCPort = nesCoordinator->startCoordinator(/**blocking**/ false);
        //Add all logical sources
        checkAndAddLogicalSources();

        for (auto& workerConf : testHarnessWorkerConfigurations) {

            //Fetch the worker configuration
            auto workerConfiguration = workerConf->getWorkerConfiguration();

            //Set ports at runtime
            workerConfiguration->setCoordinatorPort(coordinatorRPCPort);
            workerConfiguration->setCoordinatorIp(coordinatorIPAddress);

            switch (workerConf->getSourceType()) {
                case TestHarnessWorkerConfiguration::MemorySource: {
                    auto physicalSource = createPhysicalSourceOfMemoryType(workerConf);
                    workerConfiguration->addPhysicalSource(physicalSource);
                    break;
                }
                default: break;
            }

            NesWorkerPtr nesWorker = std::make_shared<NesWorker>(std::move(workerConfiguration));
            nesWorker->start(/**blocking**/ false, /**withConnect**/ true);

            //We are assuming that coordinator has a node id 1
            nesWorker->replaceParent(1, workerConfiguration->getParentId()->getValue());

            //Add Nes Worker to the configuration.
            //Note: this is required to stop the NesWorker at the end of the test
            workerConf->setNesWorker(nesWorker);

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
            throw Exception(
                "Make sure to call first validate() and then setupTopology() to the test harness before checking the output");
        }

        QueryServicePtr queryService = nesCoordinator->getQueryService();
        QueryCatalogPtr queryCatalog = nesCoordinator->getQueryCatalog();

        // local fs
        std::string filePath = testHarnessResourcePath / "testHarness.out";
        remove(filePath.c_str());

        //register query
        std::string queryString =
            queryWithoutSink + R"(.sink(FileSinkDescriptor::create(")" + filePath + R"(" , "NES_FORMAT", "APPEND"));)";
        auto faultToleranceMode = stringToFaultToleranceTypeMap(faultTolerance);
        if (faultToleranceMode == FaultToleranceType::INVALID) {
            NES_THROW_RUNTIME_ERROR("TestHarness: unable to identify fault tolerance guarantee");
        }
        auto lineageMode = stringToLineageTypeMap(lineage);
        if (lineageMode == LineageType::INVALID) {
            NES_THROW_RUNTIME_ERROR("TestHarness: unable to find given lineage type");
        }
        QueryId queryId = queryService->validateAndQueueAddRequest(queryString,
                                                                   std::move(placementStrategyName),
                                                                   faultToleranceMode,
                                                                   lineageMode);

        if (!TestUtils::waitForQueryToStart(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
        }

        // Check if the size of output struct match with the size of output schema
        // Output struct might be padded, in this case the size is not equal to the total size of its field
        // Currently, we need to produce a result with the schema that does not cause the associated struct to be padded
        // (e.g., the size is multiple of 8)
        uint64_t outputSchemaSizeInBytes = queryCatalog->getQueryCatalogEntry(queryId)
                                               ->getExecutedQueryPlan()
                                               ->getSinkOperators()[0]
                                               ->getOutputSchema()
                                               ->getSchemaSizeInBytes();
        NES_DEBUG("TestHarness: outputSchema: " << queryCatalog->getQueryCatalogEntry(queryId)
                                                       ->getInputQueryPlan()
                                                       ->getSinkOperators()[0]
                                                       ->getOutputSchema()
                                                       ->toString());
        NES_ASSERT(outputSchemaSizeInBytes == sizeof(T),
                   "The size of output struct does not match output schema."
                   " Output struct:"
                       << std::to_string(sizeof(T)) << " Schema:" << std::to_string(outputSchemaSizeInBytes));

        if (!TestUtils::checkBinaryOutputContentLengthOrTimeout<T>(numberOfContentToExpect, filePath, testTimeout)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkBinaryOutputContentLengthOrTimeout returns false, "
                                    "number of buffers to expect="
                                    << std::to_string(numberOfContentToExpect));
        }

        NES_INFO("QueryDeploymentTest: Remove query");
        if (!queryService->validateAndQueueStopRequest(queryId)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: cannot validateAndQueueStopRequest for query with id=" << queryId);
        }
        if (!TestUtils::checkStoppedOrTimeout(queryId, queryCatalog)) {
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
        auto* buff = reinterpret_cast<char*>(malloc(length));
        ifs.read(buff, length);
        std::vector<T> actualOutput(reinterpret_cast<T*>(buff), reinterpret_cast<T*>(buff) + length / sizeof(T));

        for (const auto& worker : testHarnessWorkerConfigurations) {
            worker->getNesWorker()->stop(false);
        }
        nesCoordinator->stopCoordinator(false);

        return actualOutput;
    }

    TopologyPtr getTopology() {

        if (!validationDone && !topologySetupDone) {
            throw Exception(
                "Make sure to call first validate() and then setupTopology() to the test harness before checking the output");
        }
        return nesCoordinator->getTopology();
    };

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
};
}// namespace NES

#endif  // NES_INCLUDE_UTIL_TESTHARNESS_TESTHARNESS_HPP_
