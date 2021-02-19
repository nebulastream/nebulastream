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
#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <util/E2EBase.hpp>

const uint64_t NUMBER_OF_BUFFER_TO_PRODUCE = 1000000;//600000
const uint64_t EXPERIMENT_RUNTIME_IN_SECONDS = 3;
const uint64_t EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS = 1;

const NES::DebugLevel DEBUGL_LEVEL = NES::LOG_WARNING;
const uint64_t NUMBER_OF_BUFFERS_IN_BUFFER_MANAGER = 1048576 * 4;
const uint64_t BUFFER_SIZE_IN_BYTES = 4096;

static uint64_t portOffset = 13;

//TODO: remove this once we can access NES_ASSERT without
using namespace NES;

std::string E2EBase::getInputOutputModeAsString(E2EBase::InputOutputMode mode) {
    if (mode == E2EBase::InputOutputMode::FileMode) {
        return "FileMode";
    } else if (mode == E2EBase::InputOutputMode::MemoryMode) {
        return "MemoryMode";
    } else {
        return "Unkown mode";
    }
}

std::string E2EBase::runExperiment(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt,
                                   InputOutputMode mode, std::string query) {
    std::cout << "setup experiment with parameter threadCntWorker=" << threadCntWorker
              << " threadCntCoordinator=" << threadCntCoordinator << " sourceCnt=" << sourceCnt
              << " mode=" << getInputOutputModeAsString(mode) << " query=" << query << std::endl;
    E2EBasePtr test = std::make_shared<E2EBase>(threadCntWorker, threadCntCoordinator, sourceCnt, mode);

    std::cout << "run query" << std::endl;
    test->runQuery(query);

    std::cout << "E2EBase: output result" << std::endl;
    auto res = test->getResult();
    return res;
}

E2EBase::E2EBase(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt, InputOutputMode mode)
    : numberOfWorkerThreads(threadCntWorker), numberOfCoordinatorThreads(threadCntCoordinator), sourceCnt(sourceCnt), mode(mode) {
    std::cout << "run with configuration:"
              << " threadCntWorker=" << numberOfWorkerThreads << " threadCntCoordinator=" << numberOfCoordinatorThreads
              << " sourceCnt=" << sourceCnt << " mode=" << getInputOutputModeAsString(mode);
    setup();
}

void E2EBase::recordStatistics(NES::NodeEngine::NodeEnginePtr nodeEngine) {
    for (uint64_t i = 0; i < EXPERIMENT_RUNTIME_IN_SECONDS + 1; ++i) {
        int64_t nextPeriodStartTime = EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(queryId);
        for (auto it : queryStatisticsPtrs) {
            NES::NodeEngine::QueryStatisticsPtr currentStat = std::make_shared<NES::NodeEngine::QueryStatistics>();
            currentStat->setProcessedBuffers(it->getProcessedBuffers());
            currentStat->setProcessedTasks(it->getProcessedTasks());
            currentStat->setProcessedTuple(it->getProcessedTuple());
            auto start = std::chrono::system_clock::now();
            auto in_time_t = std::chrono::system_clock::to_time_t(start);
            std::cout << "Statistics at " << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X")
                << " measurement=" << it->getQueryStatisticsAsString() << std::endl;
            if (currentStat->getProcessedTuple() == 0) {
                NES_WARNING("we already consumed all data size=" << statisticsVec.size());
            }
            else
            {
                statisticsVec.push_back(currentStat);
            }
        }

        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
    }
}

E2EBase::~E2EBase() {
    std::cout << "~E2EBase" << std::endl;
    tearDown();
    crd.reset();
    wrk1.reset();
    statisticsVec.clear();
    queryService.reset();
    queryCatalog.reset();
    for (auto ptr : memoryAreas) {
        delete ptr;
    }
}

void E2EBase::setupSources() {
    schema = NES::Schema::create()
                 ->addField(createField("id", NES::UINT64))
                 ->addField(createField("value", NES::UINT64))
                 ->addField(createField("timestamp", NES::UINT64));

    portOffset *= portOffset + 123;
    //register logical stream qnv
    std::string input =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "input.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();
    wrk1->registerLogicalStream("input", testSchemaFileName);

    if (mode == InputOutputMode::FileMode) {
        std::cout << "file source mode" << std::endl;
        NES::SourceConfigPtr srcConf = NES::SourceConfig::create();
        srcConf->setSourceType("CSVSource");
        srcConf->setSourceConfig("../tests/test_data/benchmark.csv");
        srcConf->setNumberOfTuplesToProducePerBuffer(0);
        srcConf->setSourceFrequency(0);
        srcConf->setNumberOfBuffersToProduce(NUMBER_OF_BUFFER_TO_PRODUCE);
        srcConf->setLogicalStreamName("input");
        srcConf->setSkipHeader(true);
        //register physical stream
        for (uint64_t i = 0; i < sourceCnt; i++) {
            srcConf->setPhysicalStreamName("test_stream" + std::to_string(i));
            NES::PhysicalStreamConfigPtr inputStream = NES::PhysicalStreamConfig::create(srcConf);
            wrk1->registerPhysicalStream(inputStream);
        }
    } else if (mode == InputOutputMode::MemoryMode) {
        std::cout << "memory source mode" << std::endl;
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        for (uint64_t i = 0; i < sourceCnt; i++) {
            //we put 170 tuples of size 24 Byte into a 4KB buffer
            auto memAreaSize = sizeof(Record) * 170;//nearly full buffer
            auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
            memoryAreas.push_back(memArea);
            auto* records = reinterpret_cast<Record*>(memArea);
            size_t recordSize = schema->getSchemaSizeInBytes();
            size_t numRecords = memAreaSize / recordSize;
            for (auto i = 0u; i < numRecords; ++i) {
                records[i].id = i;
                //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                records[i].value = i % 10;
                records[i].timestamp = i;
            }

            NES::AbstractPhysicalStreamConfigPtr conf = NES::MemorySourceStreamConfig::create(
                "MemorySource", "test_stream", "input", memArea, memAreaSize, NUMBER_OF_BUFFER_TO_PRODUCE, 0);

            wrk1->registerPhysicalStream(conf);
        }
    }
}
void E2EBase::setup() {
    std::cout << "setup" << std::endl;

    NES::setupLogging("E2EBase.log", DEBUGL_LEVEL);
    std::cout << "Setup E2EBase test class." << std::endl;

    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setNumWorkerThreads(numberOfCoordinatorThreads);
    crdConf->setNumberOfBuffers(NUMBER_OF_BUFFERS_IN_BUFFER_MANAGER);
    crdConf->setBufferSizeInBytes(BUFFER_SIZE_IN_BYTES);
    crdConf->setRpcPort(4000 + portOffset);
    crdConf->setRestPort(8081 + portOffset);

    std::cout << "E2EBase: Start coordinator" << std::endl;
    crd = std::make_shared<NES::NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);

    std::cout << "E2EBase: Start worker 1" << std::endl;
    NES::WorkerConfigPtr wrkConf = NES::WorkerConfig::create();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10 + portOffset);
    wrkConf->setDataPort(port + 11 + portOffset);
    wrkConf->setNumWorkerThreads(numberOfWorkerThreads);
    wrkConf->setNumberOfBuffers(NUMBER_OF_BUFFERS_IN_BUFFER_MANAGER);
    wrkConf->setBufferSizeInBytes(BUFFER_SIZE_IN_BYTES);
    wrk1 = std::make_shared<NES::NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    NES_ASSERT(retStart1, "retStart1");

    setupSources();

    queryService = crd->getQueryService();
    queryCatalog = crd->getQueryCatalog();
}

void E2EBase::runQuery(std::string query) {
    std::cout << "E2EBase: Submit query=" << query << std::endl;
    queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_ASSERT(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");

    //give the system some seconds to come to steady mode
    sleep(EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS);

    auto start = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(start);
    std::cout << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X") << " E2EBase: Run Measurement for query id=" << queryId
              << std::endl;
    recordStatistics(wrk1->getNodeEngine());

    auto stop = std::chrono::system_clock::now();
    auto out_time_t = std::chrono::system_clock::to_time_t(stop);

    std::cout << std::put_time(std::localtime(&out_time_t), "%Y-%m-%d %X")
              << " E2EBase: Finished Measurement for query id=" << queryId << std::endl;

    runtime = std::chrono::duration_cast<std::chrono::nanoseconds>(stop.time_since_epoch() - start.time_since_epoch());
}

//std::string E2EBase::outputResults(std::string outputFile) {
std::string E2EBase::getResult() {
    std::stringstream out;
    std::cout << "aggregate " << statisticsVec.size() << " measurements" << std::endl;

    NES_ASSERT(statisticsVec.size() != 0, "stats too small");
    //make diff
    //    for (int i = statisticsVec.size() - 1; i > 1; --i) {
    //        statisticsVec[i]->setProcessedTuple(statisticsVec[i]->getProcessedTuple() - statisticsVec[i - 1]->getProcessedTuple());
    //        statisticsVec[i]->setProcessedBuffers(statisticsVec[i]->getProcessedBuffers()
    //                                              - statisticsVec[i - 1]->getProcessedBuffers());
    //        statisticsVec[i]->setProcessedTasks(statisticsVec[i]->getProcessedTasks() - statisticsVec[i - 1]->getProcessedTasks());
    //    }
    //
    //    for (uint64_t i = 1; i < statisticsVec.size(); i++) {
    //        statisticsVec[0]->setProcessedBuffers(statisticsVec[0]->getProcessedBuffers() + statisticsVec[i]->getProcessedBuffers());
    //        statisticsVec[0]->setProcessedTasks(statisticsVec[0]->getProcessedTasks() + statisticsVec[i]->getProcessedTasks());
    //        statisticsVec[0]->setProcessedTuple(statisticsVec[0]->getProcessedTuple() + statisticsVec[i]->getProcessedTuple());
    //    }
    //
    //    std::cout << "sum is " << statisticsVec[0]->getQueryStatisticsAsString() << std::endl;
    //    NES_ASSERT(statisticsVec[0]->getProcessedTuple() > 0, "wrong number of tuples processed");
    //    statisticsVec[0]->setProcessedBuffers(statisticsVec[0]->getProcessedBuffers() / statisticsVec.size());
    //    statisticsVec[0]->setProcessedTasks(statisticsVec[0]->getProcessedTasks() / statisticsVec.size());
    //    statisticsVec[0]->setProcessedTuple(statisticsVec[0]->getProcessedTuple() / statisticsVec.size());
    //    std::cout << "agg is " << statisticsVec[0]->getQueryStatisticsAsString() << std::endl;
    //    out << "," << statisticsVec[0]->getProcessedBuffers() << "," << statisticsVec[0]->getProcessedTasks() << ","
    //        << statisticsVec[0]->getProcessedTuple() << "," << statisticsVec[0]->getProcessedTuple() * schema->getSchemaSizeInBytes()
    //        << std::endl;

    auto tuplesProcessd = statisticsVec[statisticsVec.size() - 1]->getProcessedTuple() - statisticsVec[0]->getProcessedTuple();
    auto bufferProcessed =
        statisticsVec[statisticsVec.size() - 1]->getProcessedBuffers() - statisticsVec[0]->getProcessedBuffers();
    auto tasksProcessed = statisticsVec[statisticsVec.size() - 1]->getProcessedTasks() - statisticsVec[0]->getProcessedTasks();

    out << "," << bufferProcessed << "," << tasksProcessed << "," << tuplesProcessd << ","
        << tuplesProcessd * schema->getSchemaSizeInBytes() << "," << tuplesProcessd * 1'000'000'000.0 / runtime.count() << ","
        << (tuplesProcessd * schema->getSchemaSizeInBytes() * 1'000'000'000.0) / runtime.count() << std::endl;

    std::cout << "runtime in sec=" << runtime.count() << std::endl;
    return out.str();
}

void E2EBase::tearDown() {
    std::cout << "E2EBase: Remove query" << std::endl;
    NES_ASSERT(queryService->validateAndQueueStopRequest(queryId), "no vaild stop quest");
    std::cout << "E2EBase: wait for stop" << std::endl;
    NES_ASSERT(NES::TestUtils::checkStoppedOrTimeout(queryId, queryCatalog), "check stop failed");

    std::cout << "E2EBase: Stop worker 1" << std::endl;
    bool retStopWrk1 = wrk1->stop(true);
    NES_ASSERT(retStopWrk1, "retStopWrk1");

    std::cout << "E2EBase: Stop Coordinator" << std::endl;
    bool retStopCord = crd->stopCoordinator(true);
    NES_ASSERT(retStopCord, "retStopCord");
    std::cout << "E2EBase: Test finished" << std::endl;
}