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
#include "E2EBenchmarks/E2EBenchmarkConfig.hpp"
#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <E2EBenchmarks/E2EBase.hpp>
#include <Components/NesWorker.hpp>

#include <chrono>
#include <iostream>
#include <string>

using namespace std;

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
    } else if (mode == E2EBase::InputOutputMode::WindowMode) {
        return "WindowMode";
    } else if (mode == E2EBase::InputOutputMode::JoinMode) {
        return "JoinMode";
    } else if (mode == E2EBase::InputOutputMode::Auto) {
        return "Auto";
    } else {
        return "Unknown mode";
    }
}

E2EBase::InputOutputMode E2EBase::getInputOutputModeFromString(std::string mode) {
    std::cout << "modus=" << mode << std::endl;
    if (mode == "FileMode") {
        return E2EBase::InputOutputMode::FileMode;
    } else if (mode == "CacheMode") {
        return E2EBase::InputOutputMode::CacheMode;
    } else if (mode == "MemoryMode") {
        return E2EBase::InputOutputMode::MemMode;
    } else if (mode == "WindowMode") {
        return E2EBase::InputOutputMode::WindowMode;
    } else if (mode == "JoinMode") {
        return E2EBase::InputOutputMode::JoinMode;
    } else if (mode == "Auto") {
        return E2EBase::InputOutputMode::Auto;
    } else {
        return E2EBase::InputOutputMode::UndefinedInputMode;
    }
}

std::string E2EBase::runExperiment() {

    std::cout << "run query" << std::endl;
    runQuery();

    std::cout << "E2EBase: output result" << std::endl;
    return getResult();
}

E2EBase::E2EBase(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt,
                 uint64_t numberOfBuffersInGlobalBufferManager, uint64_t numberOfBuffersPerPipeline,
                 uint64_t numberOfBuffersInSourceLocalBufferPool, uint64_t bufferSizeInBytes, E2EBenchmarkConfigPtr config)
    : numberOfWorkerThreads(threadCntWorker), numberOfCoordinatorThreads(threadCntCoordinator), sourceCnt(sourceCnt),
      numberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager),
      numberOfBuffersPerPipeline(numberOfBuffersPerPipeline),
      numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool), bufferSizeInBytes(bufferSizeInBytes),
      config(config) {
    std::cout << "run with configuration:"
              << " threadCntWorker=" << numberOfWorkerThreads << " threadCntCoordinator=" << numberOfCoordinatorThreads
              << " sourceCnt=" << sourceCnt << " numberOfBuffersInGlobalBufferManager=" << numberOfBuffersInGlobalBufferManager
              << " numberOfBuffersPerPipeline=" << numberOfBuffersPerPipeline
              << " numberOfBuffersInSourceLocalBufferPool=" << numberOfBuffersInSourceLocalBufferPool
              << " bufferSizeInBytes=" << bufferSizeInBytes << " scalability=" << config->getScalability()->getValue() << std::endl;
    setup();
}

std::chrono::nanoseconds E2EBase::recordStatistics() {
    //check for start
    bool readyToMeasure = false;
    NodeEngine::NodeEnginePtr nodeEngine = crd->getNodeEngine();

    while (!readyToMeasure) {
        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(queryId);
        for (auto iter : queryStatisticsPtrs) {
            if (iter->getProcessedTuple() != 0) {
                readyToMeasure = true;
            } else {
                std::cout << "engine not ready yet" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(config->getExperimentMeasureIntervalInSeconds()->getValue()));
            }
        }
    }

    uint64_t runCounter = 1;
    auto start = std::chrono::system_clock::now();
    auto inTime = std::chrono::system_clock::to_time_t(start);
    std::cout << std::put_time(std::localtime(&inTime), "%Y-%m-%d %X") << " E2EBase: Started Measurement for query id=" << queryId
              << std::endl;

    while (runCounter <= config->getNumberOfMeasurementsToCollect()->getValue()) {
        int64_t nextPeriodStartTime = config->getExperimentMeasureIntervalInSeconds()->getValue() * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        //get data from coordinator
        auto queryStatisticsCoordinatorPtrs = crd->getNodeEngine()->getQueryStatistics(queryId);
        for (auto iter : queryStatisticsCoordinatorPtrs) {
            auto ts = std::chrono::system_clock::now();
            auto timeNow = std::chrono::system_clock::to_time_t(ts);
            std::cout << "Statistics at coordinator " << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << " =>"
                      << iter->getQueryStatisticsAsString() << std::endl;

            //if first iteration just push the first value
            if (subPlanIdToTaskCnt.count(iter->getSubQueryId()) == 0) {
                std::cout << "first measurement runCounter= " << runCounter << " for subId=" << iter->getSubQueryId()
                          << std::endl;
                subPlanIdToTaskCnt[iter->getSubQueryId()] = iter->getProcessedTasks();
                subPlanIdToBufferCnt[iter->getSubQueryId()] = iter->getProcessedBuffers();
                subPlanIdToTupleCnt[iter->getSubQueryId()] = iter->getProcessedTuple();
                subPlanIdToLatencyCnt[iter->getSubQueryId()] = iter->getLatencySum();
            }

            if (runCounter == config->getNumberOfMeasurementsToCollect()->getValue()) {
                //if last iteration do last - first
                std::cout << "last measurement runCounter= " << runCounter << " for subId=" << iter->getSubQueryId() << std::endl;
                subPlanIdToTaskCnt[iter->getSubQueryId()] = iter->getProcessedTasks() - subPlanIdToTaskCnt[iter->getSubQueryId()];
                subPlanIdToBufferCnt[iter->getSubQueryId()] =
                    iter->getProcessedBuffers() - subPlanIdToBufferCnt[iter->getSubQueryId()];
                subPlanIdToTupleCnt[iter->getSubQueryId()] =
                    iter->getProcessedTuple() - subPlanIdToTupleCnt[iter->getSubQueryId()];
                subPlanIdToLatencyCnt[iter->getSubQueryId()] =
                    iter->getLatencySum() - subPlanIdToLatencyCnt[iter->getSubQueryId()];
            }
        }//end of for

        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime && runCounter != config->getNumberOfMeasurementsToCollect()->getValue()) {
            std::this_thread::sleep_for(std::chrono::seconds(config->getExperimentMeasureIntervalInSeconds()->getValue()));
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
        runCounter++;
    }

    auto stop = std::chrono::system_clock::now();
    auto outTime = std::chrono::system_clock::to_time_t(stop);
    std::cout << std::put_time(std::localtime(&outTime), "%Y-%m-%d %X")
              << " E2EBase: Finished Measurement for query id=" << queryId << std::endl;

    if (subPlanIdToTupleCnt.size() == 0) {
        NES_ASSERT(subPlanIdToTupleCnt.size() == config->getNumberOfMeasurementsToCollect()->getValue(),
                   "We cannot use this run as no data was measured");
    }

    std::cout << "content of map for debug" << std::endl;
    for (auto& val : subPlanIdToTupleCnt) {
        std::cout << "first=" << val.first << " second=" << val.second << std::endl;
    }

    return std::chrono::duration_cast<std::chrono::nanoseconds>(stop.time_since_epoch() - start.time_since_epoch());
}

E2EBase::~E2EBase() {
    std::cout << "~E2EBase" << std::endl;
    tearDown();
    crd.reset();
    wrk.reset();
    subPlanIdToTaskCnt.clear();
    subPlanIdToBufferCnt.clear();
    subPlanIdToTupleCnt.clear();
    subPlanIdToLatencyCnt.clear();
    queryService.reset();
    queryCatalog.reset();
}

void E2EBase::setupSources() {
    schema = NES::Schema::create()
                 ->addField(createField("id", NES::UINT64))
                 ->addField(createField("value", NES::UINT64))
                 ->addField(createField("timestamp", NES::UINT64));

    std::string input =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "input.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();

    NES_ASSERT(crd->getNesWorker()->registerLogicalStream("input", testSchemaFileName), "failed to create logical stream");
//    crd->getCoordinatorEngine()->registerLogicalStream("input", input);

    auto mode = getInputOutputModeFromString(config->getInputOutputMode()->getValue());
    auto query = config->getQuery()->getValue();
    if (mode == InputOutputMode::Auto) {
        //stateless queries use memMode
        if (query.find("join") == std::string::npos && query.find("window") == std::string::npos) {
            mode = InputOutputMode::MemMode;
        } else if (query.find("join") != std::string::npos && query.find("windowByKey") == std::string::npos) {
            mode = InputOutputMode::JoinMode;
        } else if (query.find("join") == std::string::npos && query.find("windowByKey") != std::string::npos) {
            mode = InputOutputMode::WindowMode;
        } else {
            NES_FATAL_ERROR("Modus not supported, only either stateless, or window or join queries are allowed currently");
        }
    }

    if (mode == InputOutputMode::FileMode) {
        NES_NOT_IMPLEMENTED();
        std::cout << "file source mode" << std::endl;
        NES::SourceConfigPtr srcConf = NES::SourceConfig::create();
        srcConf->setSourceType("CSVSource");
        srcConf->setSourceConfig("../tests/test_data/benchmark.csv");
        srcConf->setNumberOfTuplesToProducePerBuffer(0);
        srcConf->setSourceFrequency(0);
        srcConf->setNumberOfBuffersToProduce(config->getNumberOfBuffersToProduce()->getValue());
        srcConf->setLogicalStreamName("input");
        srcConf->setSkipHeader(true);
        //register physical stream
        for (uint64_t i = 0; i < sourceCnt; i++) {
            srcConf->setPhysicalStreamName("test_stream" + std::to_string(i));
            NES::PhysicalStreamConfigPtr inputStream = NES::PhysicalStreamConfig::create(srcConf);
            wrk->registerPhysicalStream(inputStream);
        }
    } else if (mode == InputOutputMode::CacheMode) {
        NES_NOT_IMPLEMENTED();
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

            NES::AbstractPhysicalStreamConfigPtr conf =
                NES::MemorySourceStreamConfig::create("MemorySource", "test_stream", "input", memArea, memAreaSize,
                                                      config->getNumberOfBuffersToProduce()->getValue(), 0);

            wrk->registerPhysicalStream(conf);
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
                for (auto u = 1u; u < numberOfTuplesToProduce - 1; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = u;
                }
                records[0].id = 123;
                records[0].value = 123;
                records[0].timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::high_resolution_clock::now().time_since_epoch())
                                           .count();
                return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf =
                NES::LambdaSourceStreamConfig::create("LambdaSource", "test_stream" + std::to_string(i), "input", func,
                                                      config->getNumberOfBuffersToProduce()->getValue(), 0);

            if (config->getScalability()->getValue() == "scale-out") {
                wrk->registerPhysicalStream(conf);
            } else {
                crd->getCoordinatorEngine()->registerPhysicalStream(crd->getNesWorker()->getWorkerId(), "LambdaSource", "test_stream" + std::to_string(i), "input");
                crd->getNodeEngine()->setConfig(conf);
            }
        }
    } else if (mode == InputOutputMode::WindowMode) {
        NES_NOT_IMPLEMENTED();
        std::cout << "windowmode source mode" << std::endl;

        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func = [](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t timestamp;
                };

                auto records = buffer.getBufferAs<Record>();
                auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::high_resolution_clock::now().time_since_epoch())
                              .count();
                for (auto u = 1u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = ts;
                }

                records[0].id = 123;
                records[0].value = 123;
                records[0].timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::high_resolution_clock::now().time_since_epoch())
                                           .count();
                return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf =
                NES::LambdaSourceStreamConfig::create("LambdaSource", "test_stream" + std::to_string(i), "input", func,
                                                      config->getNumberOfBuffersToProduce()->getValue(), 0);

            wrk->registerPhysicalStream(conf);
        }
    } else if (mode == InputOutputMode::JoinMode) {
        std::cout << "joinmode source mode" << std::endl;
        NES_NOT_IMPLEMENTED();
        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func1 = [](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t timestamp;
                };

                auto records = buffer.getBufferAs<Record>();
                auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::high_resolution_clock::now().time_since_epoch())
                              .count();

                for (auto u = 1u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = ts;
                }
                records[0].id = 123;
                records[0].value = 123;
                records[0].timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::high_resolution_clock::now().time_since_epoch())
                                           .count();

                return;
            };

            auto func2 = [](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t timestamp;
                };

                auto records = buffer.getBufferAs<Record>();
                auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::high_resolution_clock::now().time_since_epoch())
                              .count();
                for (auto u = 1u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = ts;
                }
                records[0].id = 123;
                records[0].value = 123;
                records[0].timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::high_resolution_clock::now().time_since_epoch())
                                           .count();
                return;
            };

            wrk->registerLogicalStream("input1", testSchemaFileName);
            wrk->registerLogicalStream("input2", testSchemaFileName);

            NES::AbstractPhysicalStreamConfigPtr conf1 = NES::LambdaSourceStreamConfig::create(
                "LambdaSource", "test_stream1", "input1", func1, config->getNumberOfBuffersToProduce()->getValue(), 0);
            wrk->registerPhysicalStream(conf1);

            NES::AbstractPhysicalStreamConfigPtr conf2 = NES::LambdaSourceStreamConfig::create(
                "LambdaSource", "test_stream2", "input2", func2, config->getNumberOfBuffersToProduce()->getValue(), 0);
            wrk->registerPhysicalStream(conf2);
        }
    } else {
        NES_ASSERT2_FMT(false, "input output mode not supported " << getInputOutputModeAsString(mode));
    }
}

void E2EBase::setup() {
    std::cout << "setup" << std::endl;

    portOffset += 13;

    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setNumWorkerThreads(numberOfCoordinatorThreads);
    crdConf->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
    crdConf->setnumberOfBuffersPerPipeline(numberOfBuffersPerPipeline);
    crdConf->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
    crdConf->setBufferSizeInBytes(bufferSizeInBytes);
    crdConf->setRpcPort(4000 + portOffset);
    crdConf->setRestPort(8081 + portOffset);

    std::cout << "E2EBase: Start coordinator" << std::endl;
    crd = std::make_shared<NES::NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);

    if (config->getScalability()->getValue() == "scale-out") {
        std::cout << "E2EBase: Start worker 1" << std::endl;
        NES::WorkerConfigPtr wrkConf = NES::WorkerConfig::create();
        wrkConf->setCoordinatorPort(port);
        wrkConf->setRpcPort(port + 10 + portOffset);
        wrkConf->setDataPort(port + 11 + portOffset);
        std::cout << "Cport=" << port << " CsetRpcPort=" << 4000 + portOffset << " CsetRestPort=" << 8081 + portOffset
                  << " WsetRpcPort=" << port + 10 + portOffset << " WsetDataPort=" << port + 11 + portOffset << std::endl;
        wrkConf->setNumWorkerThreads(numberOfWorkerThreads);
        wrkConf->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
        wrkConf->setnumberOfBuffersPerPipeline(numberOfBuffersPerPipeline);
        wrkConf->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
        wrkConf->setBufferSizeInBytes(bufferSizeInBytes);
        wrk = std::make_shared<NES::NesWorker>(wrkConf, NodeType::Sensor);
        bool retStart1 = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        NES_ASSERT(retStart1, "retStart1");
    }

    setupSources();

    queryService = crd->getQueryService();
    queryCatalog = crd->getQueryCatalog();
}

void E2EBase::runQuery() {

    std::cout << "E2EBase: Submit query=" << config->getQuery()->getValue() << std::endl;
    queryId = queryService->validateAndQueueAddRequest(config->getQuery()->getValue(), "BottomUp");
    NES_ASSERT(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");

    //give the system some seconds to come to steady mode
    sleep(config->getStartupSleepIntervalInSeconds()->getValue());

    runtime = recordStatistics();
}

struct space_out : std::numpunct<char> {
    char do_thousands_sep() const { return '.'; }   // separate with spaces
    std::string do_grouping() const { return "\3"; }// groups of 1 digit
};

std::string E2EBase::getResult() {
    std::stringstream out;
    //    out.precision(1);

    uint64_t tuplesProcessed = 0;
    uint64_t bufferProcessed = 0;
    uint64_t tasksProcessed = 0;
    uint64_t latencySum = 0;

    //sum up the values for worker
    for (auto& val : subPlanIdToTaskCnt) {
        tasksProcessed += val.second;
    }

    for (auto& val : subPlanIdToBufferCnt) {
        bufferProcessed += val.second;
    }

    for (auto& val : subPlanIdToTupleCnt) {
        tuplesProcessed += val.second;
    }

    for (auto& val : subPlanIdToLatencyCnt) {
        latencySum += val.second;
    }

    std::cout << "latency sum=" << latencySum << " in ms=" << std::chrono::milliseconds(latencySum).count() << std::endl;
    uint64_t runtimeInSec = std::chrono::duration_cast<std::chrono::seconds>(runtime).count();

    uint64_t throughputInTupsPerSec = tuplesProcessed / runtimeInSec;
    uint64_t throughputInMBPerSec = (tuplesProcessed * schema->getSchemaSizeInBytes() / (uint64_t) runtimeInSec) / 1024 / 1024;
    uint64_t avgLatencyInMs = latencySum / bufferProcessed;

    out << bufferProcessed << "," << tasksProcessed << "," << tuplesProcessed << ","
        << tuplesProcessed * schema->getSchemaSizeInBytes() << "," << throughputInTupsPerSec << "," << throughputInMBPerSec << ","
        << avgLatencyInMs;

    std::cout.imbue(std::locale(std::cout.getloc(), new space_out));
    std::cout << "tuples=" << tuplesProcessed << std::endl;
    std::cout << "tuples per sec=" << throughputInTupsPerSec << std::endl;
    std::cout << "runtime in sec=" << runtimeInSec << std::endl;
    std::cout << "throughput MB/se=" << throughputInMBPerSec << std::endl;
    std::cout << "avgLatencyInMs=" << avgLatencyInMs << std::endl;

    return out.str();
}

void E2EBase::tearDown() {
    try {
        std::cout << "E2EBase: Remove query" << std::endl;
        NES_ASSERT(queryService->validateAndQueueStopRequest(queryId), "no vaild stop quest");
        std::cout << "E2EBase: wait for stop" << std::endl;
        bool ret = NES::TestUtils::checkStoppedOrTimeout(queryId, queryCatalog);
        if (!ret) {
            NES_ERROR("query was not stopped within 30 sec");
        }

        std::unique_ptr<std::thread> waitThreadWorker;
        std::shared_ptr<std::promise<bool>> stopPromiseWrk = std::make_shared<std::promise<bool>>();
        if (config->getScalability()->getValue() == "scale-out") {
            std::cout << "E2EBase: Stop worker 1" << std::endl;
            waitThreadWorker = make_unique<thread>([this, stopPromiseWrk]() {
                std::future<bool> stopFutureWrk = stopPromiseWrk->get_future();
                bool satisfied = false;
                while (!satisfied) {
                    switch (stopFutureWrk.wait_for(std::chrono::seconds(1))) {
                        case future_status::ready: {
                            satisfied = true;
                        }
                        case future_status::timeout:
                        case future_status::deferred: {
                            if (wrk->isWorkerRunning()) {
                                NES_WARNING("Waiting for stop wrk cause #tasks in the queue: "
                                            << wrk->getNodeEngine()->getQueryManager()->getNumberOfTasksInWorkerQueue());
                            } else {
                                NES_WARNING("worker stopped");
                            }
                            break;
                        }
                    }
                }
            });
        }

        std::shared_ptr<std::promise<bool>> stopPromiseCord = std::make_shared<std::promise<bool>>();
        std::thread waitThreadCoordinator([this, stopPromiseCord]() {
            std::future<bool> stopFutureCord = stopPromiseCord->get_future();
            bool satisfied = false;
            while (!satisfied) {
                switch (stopFutureCord.wait_for(std::chrono::seconds(1))) {
                    case future_status::ready: {
                        satisfied = true;
                    }
                    case future_status::timeout:
                    case future_status::deferred: {
                        if (crd->isCoordinatorRunning()) {
                            NES_WARNING("Waiting for stop wrk cause #tasks in the queue: "
                                        << crd->getNodeEngine()->getQueryManager()->getNumberOfTasksInWorkerQueue());
                        } else {
                            NES_WARNING("worker stopped");
                        }
                        break;
                    }
                }
            }
        });

        if (config->getScalability()->getValue() == "scale-out") {
            bool retStopWrk = wrk->stop(true);
            stopPromiseWrk->set_value(retStopWrk);
            NES_ASSERT(retStopWrk, retStopWrk);
            waitThreadWorker->join();
        }

        std::cout << "E2EBase: Stop Coordinator" << std::endl;
        bool retStopCord = crd->stopCoordinator(true);
        stopPromiseCord->set_value(retStopCord);
        NES_ASSERT(retStopCord, retStopCord);

        waitThreadCoordinator.join();
        std::cout << "E2EBase: Test finished" << std::endl;
    } catch (...) {
        NES_ERROR("Error was thrown while query shutdown");
    }
}