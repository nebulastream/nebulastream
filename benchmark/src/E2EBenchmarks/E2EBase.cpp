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
#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <chrono>
#include <iostream>
#include <string>
#include <util/E2EBase.hpp>

using namespace std;
const uint64_t NUMBER_OF_BUFFER_TO_PRODUCE = 5000000;//5000000
const uint64_t EXPERIMENT_RUNTIME_IN_SECONDS = 5;
const uint64_t EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS = 1;

const NES::DebugLevel DEBUGL_LEVEL = NES::LOG_WARNING;
const uint64_t NUMBER_OF_BUFFERS_IN_BUFFER_MANAGER = 1048576;
const uint64_t BUFFER_SIZE_IN_BYTES = 4096;

static uint64_t portOffset = 13;

string E2EBase::getTsInRfc3339() {
    const auto now_ms = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    const auto now_s = time_point_cast<std::chrono::seconds>(now_ms);
    const auto millis = now_ms - now_s;
    const auto c_now = std::chrono::system_clock::to_time_t(now_s);

    stringstream ss;
    ss << put_time(gmtime(&c_now), "%FT%T") << '.' << setfill('0') << setw(3) << millis.count() << 'Z';
    return ss.str();
}

std::string E2EBase::getInputOutputModeAsString(E2EBase::InputOutputMode mode) {
    if (mode == E2EBase::InputOutputMode::FileMode) {
        return "FileMode";
    } else if (mode == E2EBase::InputOutputMode::CacheMode) {
        return "CacheMode";
    } else if (mode == E2EBase::InputOutputMode::MemMode) {
        return "MemoryMode";
    } else {
        return "Unkown mode";
    }
}

std::string E2EBase::runExperiment(std::string query) {
    std::cout << "setup experiment with parameter threadCntWorker=" << numberOfWorkerThreads
              << " threadCntCoordinator=" << numberOfCoordinatorThreads << " sourceCnt=" << sourceCnt
              << " mode=" << getInputOutputModeAsString(mode) << " query=" << query << std::endl;
    //    E2EBasePtr test = std::make_shared<E2EBase>(threadCntWorker, threadCntCoordinator, sourceCnt, mode);

    std::cout << "run query" << std::endl;
    runQuery(query);

    std::cout << "E2EBase: output result" << std::endl;
    return getResult();
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

            if (currentStat->getProcessedTuple() == 0) {
                NES_ERROR("No Output produced, all data size=" << statisticsVec.size());
            } else {
                if (statisticsVec.size() > 1) {
                    auto prev = statisticsVec.back();
                    std::cout << "Statistics at " << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X")
                              << " processedBuffers=" << currentStat->getProcessedBuffers() - prev->getProcessedBuffers()
                              << " processedTasks=" << currentStat->getProcessedTasks() - prev->getProcessedTasks()
                              << " processedTuples=" << currentStat->getProcessedTuple() - prev->getProcessedTuple() << std::endl;
                }
                statisticsVec.push_back(currentStat);
            }
        }

        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime) {
            std::this_thread::sleep_for(std::chrono::seconds(EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS));
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
    }//end of for
    if(statisticsVec.size() == 0)
    {
        NES_ERROR("We cannot use this run as no data was measured");
        assert(0);
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
//    NES::shutdownLogging();
}

void E2EBase::setupSources() {
    schema = NES::Schema::create()
                 ->addField(createField("id", NES::UINT64))
                 ->addField(createField("value", NES::UINT64))
                 ->addField(createField("timestamp", NES::UINT64));

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
    } else if (mode == InputOutputMode::CacheMode) {
        std::cout << "cache mode" << std::endl;
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
            for (auto u = 0u; u < numRecords; ++u) {
                records[u].id = i;
                //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                records[u].value = u % 10;
                records[u].timestamp = u;
            }

            NES::AbstractPhysicalStreamConfigPtr conf = NES::MemorySourceStreamConfig::create(
                "MemorySource", "test_stream", "input", memArea, memAreaSize, NUMBER_OF_BUFFER_TO_PRODUCE, 0);

            wrk1->registerPhysicalStream(conf);
        }
    } else if (mode == InputOutputMode::MemMode) {
        std::cout << "memory source mode" << std::endl;

        for (uint64_t i = 0; i < sourceCnt; i++) {

            auto func = [](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t timestamp;
                };

                auto records = buffer.getBufferAs<Record>();
                //                auto ts = time(0);//TODO activate this later if we really use it
                for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = u;
                }
                return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf = NES::LambdaSourceStreamConfig::create(
                "LambdaSource", "test_stream", "input", func, NUMBER_OF_BUFFER_TO_PRODUCE, 0);

            wrk1->registerPhysicalStream(conf);
        }
    } else {
        NES_ASSERT(false, "input output mode not supported");
    }
}

void E2EBase::setup() {
    std::cout << "setup" << std::endl;

    NES::setupLogging("E2EBase.log", DEBUGL_LEVEL);
    std::cout << "Setup E2EBase test class." << std::endl;

    portOffset += 13;

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
    std::cout << "Cport=" << port << " CsetRpcPort=" << 4000 + portOffset << " CsetRestPort=" << 8081 + portOffset
              << " WsetRpcPort=" << port + 10 + portOffset << " WsetDataPort=" << port + 11 + portOffset << std::endl;
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
    sleep(3);

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

struct dotted : std::numpunct<char> {
    char do_thousands_sep() const { return '.'; }   // separate with dots
    std::string do_grouping() const { return "\3"; }// groups of 3 digits
    static void imbue(std::ostream& os) { os.imbue(std::locale(os.getloc(), new dotted)); }
};

std::string E2EBase::getResult() {
    std::stringstream out;
    out.precision(1);
    std::cout << "aggregate " << statisticsVec.size() << " measurements" << std::endl;
    if (statisticsVec.size() == 0) {
        NES_ERROR("result is empty");
        return "X,X,X,X \n";
    }

    NES_ASSERT(statisticsVec.size() != 0, "stats too small");

    auto tuplesProcessed = statisticsVec[statisticsVec.size() - 1]->getProcessedTuple() - statisticsVec[0]->getProcessedTuple();
    auto bufferProcessed =
        statisticsVec[statisticsVec.size() - 1]->getProcessedBuffers() - statisticsVec[0]->getProcessedBuffers();
    auto tasksProcessed = statisticsVec[statisticsVec.size() - 1]->getProcessedTasks() - statisticsVec[0]->getProcessedTasks();

    out << "," << bufferProcessed << "," << tasksProcessed << "," << tuplesProcessed << ","
        << tuplesProcessed * schema->getSchemaSizeInBytes() << "," << std::fixed
        << tuplesProcessed * 1'000'000'000.0 / runtime.count() << "," << std::fixed
        << (tuplesProcessed * schema->getSchemaSizeInBytes() * 1'000'000'000.0) / runtime.count() / 1024 / 1024 << std::endl;

    std::cout << "runtime in sec=" << runtime.count() << std::endl;
    return out.str();
}

void E2EBase::tearDown() {
    std::cout << "E2EBase: Remove query" << std::endl;
    NES_ASSERT(queryService->validateAndQueueStopRequest(queryId), "no vaild stop quest");
    std::cout << "E2EBase: wait for stop" << std::endl;
    NES::TestUtils::checkStoppedOrTimeout(queryId, queryCatalog);

    std::cout << "E2EBase: Stop worker 1" << std::endl;
    bool retStopWrk1 = wrk1->stop(true);
    NES_ASSERT(retStopWrk1, "retStopWrk1");

    std::cout << "E2EBase: Stop Coordinator" << std::endl;
    bool retStopCord = crd->stopCoordinator(true);
    NES_ASSERT(retStopCord, "retStopCord");
    std::cout << "E2EBase: Test finished" << std::endl;
}