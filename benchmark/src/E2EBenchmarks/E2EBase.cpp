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
#include "util/E2EBenchmarkConfig.hpp"
#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <chrono>
#include <iostream>
#include <string>
#include <util/E2EBase.hpp>

using namespace std;

const uint64_t EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS = 1;
const uint64_t STARTUP_SLEEP_INTERVAL_IN_SECONDS = 3;
const uint64_t NUMBER_OF_MEASUREMENTS_TO_COLLECT = 5;

const uint64_t NUMBER_OF_BUFFERS_IN_BUFFER_MANAGER = 1048576;
const uint64_t BUFFER_SIZE_IN_BYTES = 4096;
const uint64_t NUMBER_OF_BUFFER_TO_PRODUCE = 5000000;//5000000

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
    } else {
        return "Unknown mode";
    }
}

E2EBase::InputOutputMode E2EBase::getInputOutputModeFromString(std::string mode) {
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
    } else {
        return E2EBase::InputOutputMode::UndefinedInputMode;
    }
}

std::string E2EBase::runExperiment() {
    std::cout << "setup experiment with parameter threadCntWorker=" << numberOfWorkerThreads
              << " threadCntCoordinator=" << numberOfCoordinatorThreads << " sourceCnt=" << sourceCnt
              << " mode=" << getInputOutputModeAsString(mode) << " query=" << query << std::endl;

    std::cout << "run query" << std::endl;
    runQuery();

    std::cout << "E2EBase: output result" << std::endl;
    return getResult();
}

E2EBase::E2EBase(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt, InputOutputMode mode, std::string query)
    : numberOfWorkerThreads(threadCntWorker), numberOfCoordinatorThreads(threadCntCoordinator), sourceCnt(sourceCnt), mode(mode), query(query) {
    std::cout << "run with configuration:"
              << " threadCntWorker=" << numberOfWorkerThreads << " threadCntCoordinator=" << numberOfCoordinatorThreads
              << " sourceCnt=" << sourceCnt << " mode=" << getInputOutputModeAsString(mode)
        << " query=" << query << std::endl;
    setup();
}

std::chrono::nanoseconds E2EBase::recordStatistics(NES::NodeEngine::NodeEnginePtr nodeEngine) {
    //check for start
    bool readyToMeasure = false;

    while (!readyToMeasure) {
        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(queryId);
        for (auto iter : queryStatisticsPtrs) {
            if (iter->getProcessedTuple() != 0) {
                readyToMeasure = true;
            } else {
                std::cout << "engine not ready yet" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS));
            }
        }
    }

    uint64_t runCounter = 0;
    auto start = std::chrono::system_clock::now();
    auto inTime = std::chrono::system_clock::to_time_t(start);
    std::cout << std::put_time(std::localtime(&inTime), "%Y-%m-%d %X") << " E2EBase: Started Measurement for query id=" << queryId
              << std::endl;

    while (runCounter < NUMBER_OF_MEASUREMENTS_TO_COLLECT) {
        int64_t nextPeriodStartTime = EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(queryId);
        for (auto iter : queryStatisticsPtrs) {
            auto ts = std::chrono::system_clock::now();
            auto timeNow = std::chrono::system_clock::to_time_t(ts);
            std::cout << "Statistics  at " << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << " =>"
                      << iter->getQueryStatisticsAsString() << std::endl;

            runCounter++;
            //if first iteration just push the first value
            if (subPlanIdToTaskCnt.count(iter->getSubQueryId()) == 0) {
                subPlanIdToTaskCnt[iter->getSubQueryId()] = iter->getProcessedTasks();
                subPlanIdToBufferCnt[iter->getSubQueryId()] = iter->getProcessedBuffers();
                subPlanIdToTuplelCnt[iter->getSubQueryId()] = iter->getProcessedTuple();
            }

            //if last iteration do last - first
            if (runCounter) {
                subPlanIdToTaskCnt[iter->getSubQueryId()] = iter->getProcessedTasks() - subPlanIdToTaskCnt[iter->getSubQueryId()];
                subPlanIdToBufferCnt[iter->getSubQueryId()] =
                    iter->getProcessedBuffers() - subPlanIdToBufferCnt[iter->getSubQueryId()];
                subPlanIdToTuplelCnt[iter->getSubQueryId()] =
                    iter->getProcessedTuple() - subPlanIdToTuplelCnt[iter->getSubQueryId()];
            }
        }

        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime && runCounter != NUMBER_OF_MEASUREMENTS_TO_COLLECT) {
            std::this_thread::sleep_for(std::chrono::seconds(EXPERIMENT_MEARSUREMENT_INTERVAL_IN_SECONDS));
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
    }

    auto stop = std::chrono::system_clock::now();
    auto outTime = std::chrono::system_clock::to_time_t(stop);
    std::cout << std::put_time(std::localtime(&outTime), "%Y-%m-%d %X")
              << " E2EBase: Finished Measurement for query id=" << queryId << std::endl;

    if (subPlanIdToTuplelCnt.size() == 0) {
        NES_ASSERT(subPlanIdToTuplelCnt.size() == NUMBER_OF_MEASUREMENTS_TO_COLLECT,
                   "We cannot use this run as no data was measured");
    }

    return std::chrono::duration_cast<std::chrono::nanoseconds>(stop.time_since_epoch() - start.time_since_epoch());
}

E2EBase::~E2EBase() {
    std::cout << "~E2EBase" << std::endl;
    tearDown();
    crd.reset();
    wrk1.reset();
    subPlanIdToTaskCnt.clear();
    subPlanIdToBufferCnt.clear();
    subPlanIdToTuplelCnt.clear();
    queryService.reset();
    queryCatalog.reset();
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

    if(mode == InputOutputMode::Auto)
    {
        //stateless queries use memMode
        if(query.find("join") == std::string::npos && query.find("window") == std::string::npos)
        {
            mode = InputOutputMode::MemMode;
        }
        else if(query.find("join") == std::string::npos && query.find("window") != std::string::npos)
        {
            mode = InputOutputMode::JoinMode;
        }
        else if(query.find("join") != std::string::npos && query.find("window") == std::string::npos)
        {
            mode = InputOutputMode::WindowMode;
        }
        else
        {
            NES_FATAL_ERROR("Modus not supported, only either stateless, or window or join queries are allowed currently");
        }
    }

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
                "LambdaSource", "test_stream" + std::to_string(i), "input", func, NUMBER_OF_BUFFER_TO_PRODUCE, 0);

            wrk1->registerPhysicalStream(conf);
        }
    } else if (mode == InputOutputMode::WindowMode) {
        std::cout << "windowmode source mode" << std::endl;

        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func = [](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t timestamp;
                };

                auto records = buffer.getBufferAs<Record>();
                auto ts = time(0);
                for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = ts;
                }
                return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf = NES::LambdaSourceStreamConfig::create(
                "LambdaSource", "test_stream" + std::to_string(i), "input", func, NUMBER_OF_BUFFER_TO_PRODUCE, 0);

            wrk1->registerPhysicalStream(conf);
        }
    } else if (mode == InputOutputMode::JoinMode) {
        std::cout << "joinmode source mode" << std::endl;

        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func1 = [](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t timestamp;
                };

                auto records = buffer.getBufferAs<Record>();
                auto ts = time(0);
                for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = ts;
                }
                return;
            };

            auto func2 = [](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t timestamp;
                };

                auto records = buffer.getBufferAs<Record>();
                auto ts = time(0);
                for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = u;
                    //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                    records[u].value = u % 10;
                    records[u].timestamp = ts;
                }
                return;
            };

            wrk1->registerLogicalStream("input1", testSchemaFileName);
            wrk1->registerLogicalStream("input2", testSchemaFileName);

            NES::AbstractPhysicalStreamConfigPtr conf1 = NES::LambdaSourceStreamConfig::create(
                "LambdaSource", "test_stream1", "input1", func1, NUMBER_OF_BUFFER_TO_PRODUCE, 0);
            wrk1->registerPhysicalStream(conf1);

            NES::AbstractPhysicalStreamConfigPtr conf2 = NES::LambdaSourceStreamConfig::create(
                "LambdaSource", "test_stream2", "input2", func2, NUMBER_OF_BUFFER_TO_PRODUCE, 0);
            wrk1->registerPhysicalStream(conf2);
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

void E2EBase::runQuery() {
    std::cout << "E2EBase: Submit query=" << query << std::endl;
    queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_ASSERT(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");

    //give the system some seconds to come to steady mode
    sleep(STARTUP_SLEEP_INTERVAL_IN_SECONDS);

    runtime = recordStatistics(wrk1->getNodeEngine());
}

std::string E2EBase::getResult() {
    std::stringstream out;
    out.precision(1);

    auto tuplesProcessed = 0;
    auto bufferProcessed = 0;
    auto tasksProcessed = 0;

    //sum up the values
    for (auto& val : subPlanIdToTaskCnt) {
        tasksProcessed += val.second;
    }

    for (auto& val : subPlanIdToBufferCnt) {
        bufferProcessed += val.second;
    }

    for (auto& val : subPlanIdToTuplelCnt) {
        tuplesProcessed += val.second;
    }

    out << "," << bufferProcessed << "," << tasksProcessed << "," << tuplesProcessed << ","
        << tuplesProcessed * schema->getSchemaSizeInBytes() << "," << std::fixed
        << int(tuplesProcessed * 1'000'000'000.0 / runtime.count()) << "," << std::fixed
        << (tuplesProcessed * schema->getSchemaSizeInBytes() * 1'000'000'000.0) / runtime.count() / 1024 / 1024 << std::endl;

    std::cout << "tuples per sec=" << std::fixed << int(tuplesProcessed * 1'000'000'000.0 / runtime.count()) << std::endl;
    std::cout << "runtime in sec=" << runtime.count() << std::endl;
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

        std::cout << "E2EBase: Stop worker 1" << std::endl;

        std::shared_ptr<std::promise<bool>> stopPromiseWrk1 = std::make_shared<std::promise<bool>>();
        std::shared_ptr<std::promise<bool>> stopPromiseCord = std::make_shared<std::promise<bool>>();

        std::thread t1([this, stopPromiseWrk1, stopPromiseCord]() {
            std::future<bool> stopFutureWrk1 = stopPromiseWrk1->get_future();
            bool satisfied = false;
            while (!satisfied) {
                switch (stopFutureWrk1.wait_for(std::chrono::seconds(1))) {
                    case future_status::ready: {
                        satisfied = true;
                    }
                    case future_status::timeout:
                    case future_status::deferred: {
                        if (wrk1->isWorkerRunning()) {
                            NES_WARNING("Waiting for stop wrk cause #tasks in the queue: "
                                        << wrk1->getNodeEngine()->getQueryManager()->getNumberOfTasksInWorkerQueue());
                        } else {
                            NES_WARNING("worker stopped");
                        }
                        break;
                    }
                }
            }
            std::future<bool> stopFutureCord = stopPromiseCord->get_future();
            satisfied = false;
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
        bool retStopWrk1 = wrk1->stop(true);
        stopPromiseWrk1->set_value(retStopWrk1);
        NES_ASSERT(retStopWrk1, "retStopWrk1");
        std::cout << "E2EBase: Stop Coordinator" << std::endl;
        bool retStopCord = crd->stopCoordinator(true);
        stopPromiseCord->set_value(retStopCord);
        NES_ASSERT(retStopCord, "retStopCord");
        t1.join();
        std::cout << "E2EBase: Test finished" << std::endl;
    } catch (...) {
        NES_ERROR("Error was thrown while query shutdown");
    }
}