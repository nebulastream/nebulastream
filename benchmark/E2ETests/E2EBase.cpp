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
#include <E2ETests/E2EBase.hpp>

using namespace NES;

const uint64_t numberOfBufferToProduce = 1000000;
const uint64_t experimentRuntimeinSeconds = 10;
const uint64_t experimentMeasureIntervalInSeconds = 1;

const DebugLevel debugLvl = NES::LOG_DEBUG;
const uint64_t numberOfBuffersInBufferManager = 1048576;
const uint64_t bufferSizeInBytes = 4096;

std::string getInputOutputModeAsString(E2EBase::InputOutputMode mode) {
    if (mode == E2EBase::InputOutputMode::FileMode) {
        return "FileMode";
    } else if (mode == E2EBase::InputOutputMode::MemoryMode) {
        return "MemoryMode";
    } else {
        return "unkown mode";
    }
}

std::string E2EBase::runExperiment(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt, InputOutputMode mode,
                            std::string query) {
    std::cout << "setup experiment with parameter threadCntWorker=" << threadCntWorker
              << " threadCntCoordinator=" << threadCntCoordinator << " sourceCnt=" << sourceCnt
              << " mode=" << getInputOutputModeAsString(mode) << " query=" << query
              << std::endl;
    E2EBasePtr test = std::make_unique<E2EBase>(threadCntWorker, threadCntCoordinator, sourceCnt, E2EBase::InputOutputMode::FileMode);

    std::cout << "run query" << std::endl;
    test->runQuery(query);

    std::cout << "E2EBase: output result" << std::endl;
    auto res = test->getResult();
    test.reset();
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

    for (uint64_t i = 0; i < experimentRuntimeinSeconds + 1; ++i) {
        int64_t nextPeriodStartTime = experimentMeasureIntervalInSeconds * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(queryId);
        for (auto it : queryStatisticsPtrs) {
            NodeEngine::QueryStatisticsPtr currentStat = std::make_shared<NodeEngine::QueryStatistics>();
            currentStat->setProcessedBuffers(it->getProcessedBuffers());
            currentStat->setProcessedTasks(it->getProcessedTasks());
            currentStat->setProcessedTuple(it->getProcessedTuple());
            NES_WARNING("Statistics at ts=" << std::time(0) << " measurement=" << it->getQueryStatisticsAsString());
            statisticsVec.push_back(currentStat);
        }

        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime) {
            std::this_thread::sleep_for(std::chrono::microseconds(900));
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
}

void E2EBase::setupSources()
{
    schema = Schema::create()
        ->addField(createField("value", UINT64))
        ->addField(createField("id", UINT64))
        ->addField(createField("timestamp", UINT64));

    //register logical stream qnv
    std::string input =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "input.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();
    wrk1->registerLogicalStream("input", testSchemaFileName);

    SourceConfigPtr srcConf = SourceConfig::create();
    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/benchmark.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setSourceFrequency(0);
    srcConf->setNumberOfBuffersToProduce(numberOfBufferToProduce);
    srcConf->setLogicalStreamName("input");
    srcConf->setSkipHeader(true);
    //register physical stream
    for(uint64_t i = 0; i < sourceCnt; i++)
    {
        srcConf->setPhysicalStreamName("test_stream" + std::to_string(i));
        PhysicalStreamConfigPtr inputStream = PhysicalStreamConfig::create(srcConf);
        wrk1->registerPhysicalStream(inputStream);
    }
}

void E2EBase::setup() {
    std::cout << "setup" << std::endl;

    NES::setupLogging("E2EBase.log", debugLvl);
    std::cout << "Setup E2EBase test class." << std::endl;

    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    crdConf->setNumWorkerThreads(numberOfCoordinatorThreads);
    crdConf->setRpcPort(4000);
    crdConf->setRestPort(8081);

    std::cout << "E2EBase: Start coordinator" << std::endl;
    crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);

    std::cout << "E2EBase: Start worker 1" << std::endl;
    wrkConf->setCoordinatorPort(4000);
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    wrkConf->setNumWorkerThreads(numberOfWorkerThreads);
    wrkConf->setNumberOfBuffers(numberOfBuffersInBufferManager);
    wrkConf->setBufferSizeInBytes(bufferSizeInBytes);
    wrk1 = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    NES_ASSERT(retStart1, "retStart1");;

    setupSources();

    queryService = crd->getQueryService();
    queryCatalog = crd->getQueryCatalog();
}

void E2EBase::runQuery(std::string query) {
    std::cout << "E2EBase: Submit query=" << query << std::endl;
    queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_ASSERT(TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");

    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    std::cout << std::put_time(&tm, "%d-%m-%Y %H-%M-%S") << " E2EBase: Run Measurement for query id=" << queryId  << std::endl;
    recordStatistics(wrk1->getNodeEngine());
    auto t2 = std::time(nullptr);
    auto tm2 = *std::localtime(&t2);
    std::cout << std::put_time(&tm2, "%d-%m-%Y %H-%M-%S") << " E2EBase: Finished Measurement for query id=" << queryId  << std::endl;
}

//std::string E2EBase::outputResults(std::string outputFile) {
std::string E2EBase::getResult() {
    std::stringstream out;
    std::cout << "aggregate " << statisticsVec.size() << " measurements" << std::endl;

    NES_ASSERT(statisticsVec.size() != 0, "stats too small");
    //make diff
    for (int i = statisticsVec.size() - 1; i > 1; --i) {
        statisticsVec[i]->setProcessedTuple(statisticsVec[i]->getProcessedTuple() - statisticsVec[i - 1]->getProcessedTuple());
        statisticsVec[i]->setProcessedBuffers(statisticsVec[i]->getProcessedBuffers()
                                                  - statisticsVec[i - 1]->getProcessedBuffers());
        statisticsVec[i]->setProcessedTasks(statisticsVec[i]->getProcessedTasks() - statisticsVec[i - 1]->getProcessedTasks());
    }

    for (uint64_t i = 1; i < statisticsVec.size(); i++) {
        statisticsVec[0]->setProcessedBuffers(statisticsVec[0]->getProcessedBuffers() + statisticsVec[i]->getProcessedBuffers());
        statisticsVec[0]->setProcessedTasks(statisticsVec[0]->getProcessedTasks() + statisticsVec[i]->getProcessedTasks());
        statisticsVec[0]->setProcessedTuple(statisticsVec[0]->getProcessedTuple() + statisticsVec[i]->getProcessedTuple());
    }
    std::cout << "sum is " << statisticsVec[0]->getQueryStatisticsAsString() << std::endl;
    NES_ASSERT(statisticsVec[0]->getProcessedTuple() > 0, "wrong number of tuples processed");
    statisticsVec[0]->setProcessedBuffers(statisticsVec[0]->getProcessedBuffers() / statisticsVec.size());
    statisticsVec[0]->setProcessedTasks(statisticsVec[0]->getProcessedTasks() / statisticsVec.size());
    statisticsVec[0]->setProcessedTuple(statisticsVec[0]->getProcessedTuple() / statisticsVec.size());
    std::cout << "agg is " << statisticsVec[0]->getQueryStatisticsAsString() << std::endl;
    out << "," << statisticsVec[0]->getProcessedBuffers()
                                                      << "," << statisticsVec[0]->getProcessedTasks()
                  << "," << statisticsVec[0]->getProcessedTuple()
                  << "," << statisticsVec[0]->getProcessedTuple() * schema->getSchemaSizeInBytes() << std::endl;

    return out.str();
}

void E2EBase::tearDown() {
    std::cout << "E2EBase: Remove query" << std::endl;
    NES_ASSERT(queryService->validateAndQueueStopRequest(queryId), "no vaild stop quest");
    NES_ASSERT(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog), "check stop failed");

    std::cout << "E2EBase: Stop worker 1" << std::endl;
    bool retStopWrk1 = wrk1->stop(true);
    NES_ASSERT(retStopWrk1, "retStopWrk1");

    std::cout << "E2EBase: Stop Coordinator" << std::endl;
    bool retStopCord = crd->stopCoordinator(true);
    NES_ASSERT(retStopCord, "retStopCord");;
    std::cout << "E2EBase: Test finished" << std::endl;
}