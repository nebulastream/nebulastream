/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_TESTHARNESS_HPP
#define NES_TESTHARNESS_HPP
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestHarness/TestHarnessWorker.hpp>
#include <Util/TestUtils.hpp>

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
                         uint16_t restPort = 8081,
                         uint16_t rpcPort = 4000,
                         uint64_t memSrcFrequency = 0,
                         uint64_t memSrcNumBuffToProcess = 1)
        : ipAddress("127.0.0.1"), queryWithoutSink(std::move(queryWithoutSink)), bufferSize(4096),
          memSrcFrequency(memSrcFrequency), memSrcNumBuffToProcess(memSrcNumBuffToProcess) {
        NES_INFO("TestHarness: Start coordinator");
        crdConf = CoordinatorConfig::create();
        crdConf->resetCoordinatorOptions();
        crdConf->setCoordinatorIp(ipAddress);
        crdConf->setRestPort(restPort);
        crdConf->setRpcPort(rpcPort);
        wrkConf = WorkerConfig::create();
        crd = std::make_shared<NesCoordinator>(crdConf);
        crdPort = crd->startCoordinator(/**blocking**/ false);
        nonSourceWorkerCount = 0;
    };

    ~TestHarness() { NES_DEBUG("TestHarness: ~TestHarness()"); };

    /**
         * @brief push a single element/tuple to specific source
         * @param element element of Record to push
         * @param sourceIdx index of the source to which the element is pushed
         */
    template<typename T>
    void pushElement(T element, uint64_t sourceIdx) {
        if (sourceIdx >= testHarnessWorkers.size()) {
            NES_THROW_RUNTIME_ERROR("TestHarness: sourceIdx is out of bound");
        }

        if (!std::is_class<T>::value) {
            NES_THROW_RUNTIME_ERROR("TestHarness: tuples must be instances of struct");
        }

        if (sizeof(T) != testHarnessWorkers.at(sourceIdx).schema->getSchemaSizeInBytes()) {
            NES_THROW_RUNTIME_ERROR("TestHarness: tuple size and schema size does not match");
        }

        auto* memArea = reinterpret_cast<uint8_t*>(malloc(sizeof(T)));
        memcpy(memArea, reinterpret_cast<uint8_t*>(&element), sizeof(T));
        testHarnessWorkers.at(sourceIdx).record.push_back(memArea);
    }

    /**
         * @brief check the schema size of the logical stream and if it already exists
         * @param logical stream name
         * @param schema schema of the source
         * @param physical stream name
         */
    void checkAndAddSource(const std::string& logicalStreamName, const SchemaPtr& schema) {
        // Check if logical stream already exists
        if (!crd->getStreamCatalog()->testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
            NES_TRACE("TestHarness: logical source does not exist in the stream catalog, adding a new logical stream "
                      << logicalStreamName);
            crd->getStreamCatalog()->addLogicalStream(logicalStreamName, schema);
        } else {
            // Check if it has the same schema
            if (!crd->getStreamCatalog()->getSchemaForLogicalStream(logicalStreamName)->equals(schema, true)) {
                NES_TRACE("TestHarness: logical source " << logicalStreamName
                                                         << " exists in the stream catalog with "
                                                            "different schema, replacing it with a new schema");
                crd->getStreamCatalog()->removeLogicalStream(logicalStreamName);
                crd->getStreamCatalog()->addLogicalStream(logicalStreamName, schema);
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
    void addMemorySource(const std::string& logicalStreamName,
                         const SchemaPtr& schema,
                         std::string physicalStreamName,
                         uint64_t parentId) {
        // set the localWorkerRpcPort and localWorkerZmqPort based on the number of workers
        wrkConf->resetWorkerOptions();
        wrkConf->setCoordinatorPort(crdPort);
        wrkConf->setRpcPort(crdPort + (testHarnessWorkers.size() + 1) * 20);
        wrkConf->setDataPort(crdPort + (testHarnessWorkers.size() + 1) * 20 + 1);
        auto wrk = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);
        wrk->replaceParent(crd->getTopology()->getRoot()->getId(), parentId);

        std::vector<uint8_t*> currentSourceRecords;

        TestHarnessWorker currentMemorySource = TestHarnessWorker();
        currentMemorySource.wrk = wrk;
        currentMemorySource.type = TestHarnessWorker::MemorySource;
        currentMemorySource.record = currentSourceRecords;
        currentMemorySource.schema = schema;
        currentMemorySource.logicalStreamName = logicalStreamName;
        currentMemorySource.physicalStreamName = std::move(physicalStreamName);

        testHarnessWorkers.push_back(currentMemorySource);

        NES_ASSERT(parentId != INVALID_TOPOLOGY_NODE_ID, "The provided ParentId is an INVALID_TOPOLOGY_NODE_ID");
        // check if record may span multiple buffers
        NES_ASSERT2_FMT(bufferSize % schema->getSchemaSizeInBytes() == 0,
                        "TestHarness: A record might span multiple buffers and this is not supported bufferSize="
                            << bufferSize << " recordSize=" << schema->getSchemaSizeInBytes());
        checkAndAddSource(logicalStreamName, schema);
    }

    /**
         * @brief add a memory source to be used in the test
         * @param logical stream name
         * @param schema schema of the source
         * @param physical stream name
         */
    void addMemorySource(std::string logicalStreamName, SchemaPtr schema, std::string physicalStreamName) {
        uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
        addMemorySource(std::move(logicalStreamName), std::move(schema), std::move(physicalStreamName), crdTopologyNodeId);
    }

    /**
         * @brief add a csv source to be used in the test and connect to parent with specific parent id
         * @param schema schema of the source
         * @param csvSourceConf physical stream configuration for the csv source
         * @param parentId id of the parent to connect
         */
    void addCSVSource(const PhysicalStreamConfigPtr& csvSourceConf, SchemaPtr schema, uint64_t parentId) {
        wrkConf->resetWorkerOptions();
        wrkConf->setCoordinatorPort(crdPort);
        wrkConf->setRpcPort(crdPort + (testHarnessWorkers.size() + 1) * 20);
        wrkConf->setDataPort(crdPort + (testHarnessWorkers.size() + 1) * 20 + 1);
        auto wrk = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);
        wrk->replaceParent(crd->getTopology()->getRoot()->getId(), parentId);

        TestHarnessWorker currentCsvSource = TestHarnessWorker();
        currentCsvSource.wrk = wrk;
        currentCsvSource.type = TestHarnessWorker::CSVSource;
        currentCsvSource.csvSourceConfig = csvSourceConf;

        testHarnessWorkers.push_back(currentCsvSource);

        checkAndAddSource(csvSourceConf->getLogicalStreamName(), std::move(schema));
    }

    /**
      * @brief add a csv source to be used in the test
      * @param schema schema of the source
      * @param csvSourceConf physical stream configuration for the csv source
      */
    void addCSVSource(PhysicalStreamConfigPtr csvSourceConf, SchemaPtr schema) {
        uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
        addCSVSource(std::move(csvSourceConf), std::move(schema), crdTopologyNodeId);
    }

    /**
     * @brief add non source worker and connect to parent with specific parent id
     * @param parentId id of the parent to connect
     */
    void addNonSourceWorker(uint64_t parentId) {
        nonSourceWorkerCount++;
        wrkConf->resetWorkerOptions();
        wrkConf->setCoordinatorPort(crdPort);
        wrkConf->setRpcPort(crdPort + (testHarnessWorkers.size() + 1) * 20);
        wrkConf->setDataPort(crdPort + (testHarnessWorkers.size() + 1) * 20 + 1);
        auto wrk = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);
        wrk->replaceParent(crd->getTopology()->getRoot()->getId(), parentId);

        TestHarnessWorker currentNonSource = TestHarnessWorker();
        currentNonSource.wrk = wrk;
        currentNonSource.type = TestHarnessWorker::NonSource;
        testHarnessWorkers.push_back(currentNonSource);
    }

    /**
         * @brief add non source worker
         */
    void addNonSourceWorker() {
        uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
        addNonSourceWorker(crdTopologyNodeId);
    }

    uint64_t getWorkerCount() { return testHarnessWorkers.size(); }

    /**
         * @brief execute the test based on the given operator, pushed elements, and number of workers,
         * then return the result of the query execution
         * @param placementStrategyName: placement strategy name
         * @param numberOfContentToExpect: total number of tuple expected in the query result
         * @return output string
         */
    template<typename T>
    std::vector<T> getOutput(uint64_t numberOfContentToExpect, std::string placementStrategyName, uint64_t testTimeout = 60) {
        uint64_t sourceCount = 0;
        for (const TestHarnessWorker& worker : testHarnessWorkers) {
            if (worker.type == TestHarnessWorker::CSVSource || worker.type == TestHarnessWorker::MemorySource) {
                sourceCount++;
            }
        }
        if (testHarnessWorkers.empty()) {
            NES_THROW_RUNTIME_ERROR("TestHarness: No worker added to the test harness");
        } else if (sourceCount == 0) {
            NES_THROW_RUNTIME_ERROR("TestHarness: No worker with source added to the test harness");
        }

        QueryServicePtr queryService = crd->getQueryService();
        QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

        for (const TestHarnessWorker& worker : testHarnessWorkers) {
            if (worker.type == TestHarnessWorker::CSVSource) {
                worker.wrk->registerPhysicalStream(worker.csvSourceConfig);
            } else if (worker.type == TestHarnessWorker::MemorySource) {
                // create and populate memory source
                auto currentSourceNumOfRecords = worker.record.size();
                auto tupleSize = worker.schema->getSchemaSizeInBytes();
                NES_DEBUG("Tuple Size: " << tupleSize);
                NES_DEBUG("currentSourceNumOfRecords: " << currentSourceNumOfRecords);
                auto memAreaSize = currentSourceNumOfRecords * tupleSize;
                auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));

                auto currentRecords = worker.record;
                for (std::size_t j = 0; j < currentSourceNumOfRecords; ++j) {
                    memcpy(&memArea[tupleSize * j], currentRecords.at(j), tupleSize);
                }

                AbstractPhysicalStreamConfigPtr conf =
                    MemorySourceStreamConfig::create("MemorySource",
                                                     worker.physicalStreamName,
                                                     worker.logicalStreamName,
                                                     memArea,
                                                     memAreaSize,
                                                     /** numberOfBuffers*/ memSrcNumBuffToProcess,
                                                     /** frequency*/ memSrcFrequency,
                                                     "frequency",
                                                     "copyBuffer");
                worker.wrk->registerPhysicalStream(conf);
            }
        }

        // local fs
        std::string filePath = "testHarness.out";
        remove(filePath.c_str());

        //register query
        std::string queryString =
            queryWithoutSink + R"(.sink(FileSinkDescriptor::create(")" + filePath + R"(" , "NES_FORMAT", "APPEND"));)";
        QueryId queryId = queryService->validateAndQueueAddRequest(queryString, std::move(placementStrategyName));

        if (!TestUtils::waitForQueryToStart(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
        }

        // Check if the size of output struct match with the size of output schema
        // Output struct might be padded, in this case the size is not equal to the total size of its field
        // Currently, we need to produce a result with the schema that does not cause the associated struct to be padded
        // (e.g., the size is multiple of 8)
        uint64_t outputSchemaSizeInBytes = queryCatalog->getQueryCatalogEntry(queryId)
                                               ->getQueryPlan()
                                               ->getSinkOperators()[0]
                                               ->getOutputSchema()
                                               ->getSchemaSizeInBytes();
        NES_DEBUG(
            "TestHarness: outputSchema: "
            << queryCatalog->getQueryCatalogEntry(queryId)->getQueryPlan()->getSinkOperators()[0]->getOutputSchema()->toString());
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

        for (const TestHarnessWorker& worker : testHarnessWorkers) {
            worker.wrk->stop(false);
        }
        crd->stopCoordinator(false);

        return actualOutput;
    }

    TopologyPtr getTopology() { return crd->getTopology(); };

    /*
     * @brief get the id of worker at a particular index
     * @param workerIdx index of the worker in the test harness
     */
    uint64_t getWorkerId(uint64_t workerIdx) { return testHarnessWorkers.at(workerIdx).wrk->getTopologyNodeId(); }

  private:
    CoordinatorConfigPtr crdConf;
    WorkerConfigPtr wrkConf;
    NesCoordinatorPtr crd;
    uint64_t crdPort;
    std::string ipAddress;

    std::string queryWithoutSink;
    uint64_t bufferSize;
    uint64_t nonSourceWorkerCount;

    uint64_t memSrcFrequency;
    uint64_t memSrcNumBuffToProcess;

    std::vector<TestHarnessWorker> testHarnessWorkers;
};
}// namespace NES

#endif//NES_TESTHARNESS_HPP
