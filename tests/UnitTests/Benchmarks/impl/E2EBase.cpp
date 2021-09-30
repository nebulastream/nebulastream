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
#include <Runtime/NodeEngine.hpp>
#include <Components/NesWorker.hpp>
#include <Services/StreamCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
//#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <E2EBase.hpp>
#include <E2EBenchmarkConfig.hpp>
#include <chrono>
#include <iostream>
#include <string>
#include "../../../util/SchemaSourceDescriptor.hpp"
//#include "../../../util/TestQuery.hpp"
//#include "../../../util/TestQueryCompiler.hpp"
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>

using namespace std;

static uint64_t portOffset = 13;

/**
 * This function splits a string by a delimiter into a vector
 * @param s
 * @param delim
 * @return
 **/
std::vector<uint64_t> split(const std::string& str, char delim) {
    std::stringstream ss(str);
    std::string item;
    std::vector<std::uint64_t> elems;
    while (std::getline(ss, item, delim)) {
        //split element by delim and push it to a vector
        elems.push_back(stoi(item));
    }
    return elems;
}



string E2EBase::getTsInRfc3339() {
    const auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    const auto now_s = std::chrono::time_point_cast<std::chrono::seconds>(now_ms);
    const auto millis = now_ms - now_s;
    const auto c_now = std::chrono::system_clock::to_time_t(now_s);

    stringstream ss;
    ss << put_time(gmtime(&c_now), "%FT%T") << '.' << setfill('0') << setw(3) << millis.count() << 'Z';
    return ss.str();
}

std::string E2EBase::getInputTypeAsString(E2EBase::InputType type) {
    if (type == E2EBase::InputType::FileMode) {
        return "FileMode";
    } else if (type == E2EBase::InputType::MemoryMode) {
        return "MemoryMode";
    } else if (type == E2EBase::InputType::LambdaMode) {
        return "LambdaMode";
    } else if (type == E2EBase::InputType::WindowMode) {
        return "WindowMode";
    } else if (type == E2EBase::InputType::YSBMode) {
        return "YSBMode";
    } else if (type == E2EBase::InputType::JoinMode) {
        return "JoinMode";
    } else if (type == E2EBase::InputType::Auto) {
        return "Auto";
    } else {
        return "Unknown type";
    }
}

E2EBase::InputType E2EBase::getInputTypeFromString(std::string type) {
    std::cout << "type=" << type << std::endl;
    if (type == "FileMode") {
        return E2EBase::InputType::FileMode;
    } else if (type == "MemoryMode") {
        return E2EBase::InputType::MemoryMode;
    } else if (type == "LambdaMode") {
        return E2EBase::InputType::LambdaMode;
    } else if (type == "WindowMode") {
        return E2EBase::InputType::WindowMode;
    } else if (type == "YSBMode") {
        return E2EBase::InputType::YSBMode;
    } else if (type == "JoinMode") {
        return E2EBase::InputType::JoinMode;
    } else if (type == "Auto") {
        return E2EBase::InputType::Auto;
    } else {
        return E2EBase::InputType::UndefinedInputType;
    }
}

std::string E2EBase::getInputModeAsString(E2EBase::InputMode mode) {
    if (mode == E2EBase::InputMode::Frequency) {
        return "frequency";
    } else if (mode == E2EBase::InputMode::IngestionRate) {
        return "ingestionrate";
    } else {
        return "Unknown type";
    }
}

E2EBase::InputMode E2EBase::getInputModeFromString(std::string mode) {
    std::cout << "mode=" << mode << std::endl;
    if (mode == "frequency") {
        return E2EBase::InputMode::Frequency;
    } else if (mode == "ingestionrate") {
        return E2EBase::InputMode::IngestionRate;
    } else {
        return E2EBase::InputMode::UndefinedInputMode;
    }
}

std::string E2EBase::runExperiment( const QueryPlanPtr& updatedQueryPlan) {
    std::cout << "run query" << std::endl;
    bool res = runQuery(updatedQueryPlan);

    if (res) {
        std::cout << "E2EBase: output result" << std::endl;
        return getResult();
    } else {
        return "invalid run";
    }
}

E2EBase::E2EBase(uint64_t threadCntWorker,
                 uint64_t sourceCnt,
                 uint64_t numberOfBuffersInGlobalBufferManager,
                 uint64_t numberOfBuffersPerPipeline,
                 uint64_t numberOfBuffersInSourceLocalBufferPool,
                 uint64_t bufferSizeInBytes,
                 uint64_t gatherValue,
                 E2EBenchmarkConfigPtr config)
    : numberOfWorkerThreads(threadCntWorker),
      sourceCnt(sourceCnt),
      numberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager),
      numberOfBuffersPerPipeline(numberOfBuffersPerPipeline),
      numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool),
      bufferSizeInBytes(bufferSizeInBytes),
      gatheringValue(gatherValue),
      config(config) {
    std::cout << "run with configuration file:" << config->toString() << std::endl;
    std::cout << "actual config: numberOfWorkerThreads=" << numberOfWorkerThreads << " sourceCnt=" << sourceCnt <<
              " numberOfBuffersInGlobalBufferManager=" << numberOfBuffersInGlobalBufferManager
              << " numberOfBuffersPerPipeline=" << numberOfBuffersPerPipeline
              << " bufferSizeInBytes=" << bufferSizeInBytes
              << " gatheringValue=" << gatheringValue << std::endl;
    setup();
}

std::chrono::nanoseconds E2EBase::recordStatistics() {
    //check for start
    bool readyToMeasure = false;
    auto nodeEngine = crd->getNodeEngine();
    auto globalRetryCnt = 0;
    while (!readyToMeasure) {
        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(queryId);
        for (auto iter : queryStatisticsPtrs) {
            if (iter->getProcessedTuple() != 0) {
                std::cout << "engine READY" << std::endl;
                readyToMeasure = true;
            } else {
                std::cout << "engine not ready yet" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(config->getExperimentMeasureIntervalInSeconds()->getValue()));
                globalRetryCnt++;
                if(globalRetryCnt == 100)
                {
                    return std::chrono::nanoseconds(0);
                }
            }
        }
    }

    uint64_t runCounter = 1;
    auto start = std::chrono::system_clock::now();
    auto inTime = std::chrono::system_clock::to_time_t(start);
    std::cout << std::put_time(std::localtime(&inTime), "%Y-%m-%d %X") << " E2EBase: Started Measurement for query id="
              << queryId
              << std::endl;

    while (runCounter <= config->getNumberOfMeasurementsToCollect()->getValue()) {
        int64_t nextPeriodStartTime = config->getExperimentMeasureIntervalInSeconds()->getValue()*1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        //get data from coordinator
        auto queryStatisticsCoordinatorPtrs = crd->getNodeEngine()->getQueryStatistics(queryId);
        for (auto iter : queryStatisticsCoordinatorPtrs) {
            auto ts = std::chrono::system_clock::now();
            auto timeNow = std::chrono::system_clock::to_time_t(ts);
            std::cout << "Statistics at coordinator " << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << " =>"
                      << iter->getQueryStatisticsAsString() << std::endl;

            //
            //if first iteration just push the first value
            if (subPlanIdToTaskCnt.count(iter->getSubQueryId()) == 0) {
                std::cout << "first measurement runCounter= " << runCounter << " for subId=" << iter->getSubQueryId()
                          << std::endl;
                subPlanIdToTaskCnt[iter->getSubQueryId()] = iter->getProcessedTasks();
                subPlanIdToBufferCnt[iter->getSubQueryId()] = iter->getProcessedBuffers();
                subPlanIdToTupleCnt[iter->getSubQueryId()] = iter->getProcessedTuple();
                subPlanIdToLatencyCnt[iter->getSubQueryId()] = iter->getLatencySum();
                subPlanIdToQueueCnt[iter->getSubQueryId()] = iter->getQueueSizeSum();
                subPlanIdToAvailableGlobalBufferCnt[iter->getSubQueryId()] = iter->getAvailableGlobalBufferSum();
                subPlanIdToAvailableFixedBufferCnt[iter->getSubQueryId()] = iter->getAvailableFixedBufferSum();
            }

            if (runCounter == config->getNumberOfMeasurementsToCollect()->getValue()) {
                //if last iteration do last - first
                std::cout << "last measurement runCounter= " << runCounter << " for subId=" << iter->getSubQueryId()
                          << std::endl;
                subPlanIdToTaskCnt[iter->getSubQueryId()] =
                    iter->getProcessedTasks() - subPlanIdToTaskCnt[iter->getSubQueryId()];
                subPlanIdToBufferCnt[iter->getSubQueryId()] =
                    iter->getProcessedBuffers() - subPlanIdToBufferCnt[iter->getSubQueryId()];
                subPlanIdToTupleCnt[iter->getSubQueryId()] =
                    iter->getProcessedTuple() - subPlanIdToTupleCnt[iter->getSubQueryId()];
                subPlanIdToLatencyCnt[iter->getSubQueryId()] =
                    iter->getLatencySum() - subPlanIdToLatencyCnt[iter->getSubQueryId()];
                subPlanIdToQueueCnt[iter->getSubQueryId()] =
                    iter->getQueueSizeSum() - subPlanIdToQueueCnt[iter->getSubQueryId()];
                subPlanIdToAvailableGlobalBufferCnt[iter->getSubQueryId()] =
                    iter->getAvailableGlobalBufferSum() - subPlanIdToAvailableGlobalBufferCnt[iter->getSubQueryId()];
                subPlanIdToAvailableFixedBufferCnt[iter->getSubQueryId()] =
                    iter->getAvailableFixedBufferSum() - subPlanIdToAvailableFixedBufferCnt[iter->getSubQueryId()];

                subPlanToTsToLatencyMap[iter->getSubQueryId()] = iter->getTsToLatencyMap();
            }
        }//end of for

        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime && runCounter != config->getNumberOfMeasurementsToCollect()->getValue()) {
            std::this_thread::sleep_for(std::chrono::seconds(config->getExperimentMeasureIntervalInSeconds()->getValue()));
            curTime =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
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
    defaultSchema = NES::Schema::create()
        ->addField(createField("id", NES::UINT64))
        ->addField(createField("value", NES::UINT64))
        ->addField(createField("payload", NES::UINT64))
        ->addField(createField("timestamp", NES::UINT64));
    std::string input =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("payload", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "defaultSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();

    NES_ASSERT(crd->getNesWorker()->registerLogicalStream("input", testSchemaFileName),
               "failed to create logical stream");

    auto inType = getInputTypeFromString(config->getInputType()->getValue());
    inType = InputType::WindowMode;//aj
    auto query = config->getQuery()->getValue();

    if (inType == InputType::FileMode) {
        NES_NOT_IMPLEMENTED();
    } else if (inType == InputType::MemoryMode) {
        std::cout << "MemoryMode mode" << std::endl;
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t payload;
            uint64_t timestamp;
        };
        std::vector<std::uint64_t> sourcePinLoations = split(config->getSourcePinList()->getValue(), ',');
        for (uint64_t i = 0; i < sourceCnt; i++) {
            void* tmp = nullptr;
            const uint64_t bufferCnt = 1000;
            const uint64_t bufferAreaSize = bufferCnt * bufferSizeInBytes;
            NES_ASSERT(posix_memalign(&tmp, 64, bufferAreaSize) == 0,
                       "memory allocation failed with alignment");
            auto* memArea = static_cast<uint8_t*>(tmp);
            memoryAreas.push_back(memArea);
            auto* records = reinterpret_cast<Record*>(memArea);
            size_t recordSize = defaultSchema->getSchemaSizeInBytes();
            size_t numRecords = std::floor(double(bufferAreaSize)/double(recordSize));

            for (auto u = 0u; u < numRecords; ++u) {
                records[u].id = u;
                records[u].value = u%100;
                records[u].payload = u;
                records[u].timestamp = u;
            }

            size_t mapLocation = sourcePinLoations.size() == 0 ? std::numeric_limits<uint64_t>::max() : sourcePinLoations[i];
            std::cout << "memsource produces tuples=" << numRecords << " on pinnedToCore=" << mapLocation << std::endl;
            NES::AbstractPhysicalStreamConfigPtr conf =
                NES::MemorySourceStreamConfig::create("MemorySource",
                                                      "test_stream",
                                                      "input",
                                                      memoryAreas[i],
                                                      bufferAreaSize,
                                                      config->getNumberOfBuffersToProduce()->getValue(),
                                                      gatheringValue,
                                                      config->getGatheringMode()->getValue(),
                                                      config->getSourceMode()->getValue(), mapLocation);

            if (config->getScalability()->getValue() == "scale-out") {
                wrk->registerPhysicalStream(conf);
            } else {



                // common case
                TopologyNodePtr physicalNode =crd->getTopology()->getRoot();
                crd->getStreamCatalogService()->registerPhysicalStream(physicalNode,
                                                                    "MemorySource",
                                                                    "test_stream" + std::to_string(i),
                                                                    "input");
                crd->getNodeEngine()->setConfig(conf);
            }
        }
    } else if (inType == InputType::LambdaMode) {
        std::cout << "LambdaMode source mode" << std::endl;

        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
              struct Record {
                  uint64_t id;
                  uint64_t value;
                  uint64_t payload;
                  uint64_t timestamp;
              };

              auto records = (Record*) buffer.getBuffer();
              for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                  records[u].id = u;
                  records[u].value = u%100;
                  records[u].payload = u;
                  records[u].timestamp = u;
              }
              return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf =
                NES::LambdaSourceStreamConfig::create("LambdaSource",
                                                      "test_stream" + std::to_string(i),
                                                      "input",
                                                      func,
                                                      config->getNumberOfBuffersToProduce()->getValue(),
                                                      gatheringValue,
                                                      config->getGatheringMode()->getValue());

            if (config->getScalability()->getValue() == "scale-out") {
                wrk->registerPhysicalStream(conf);
            } else {
                // common case
                TopologyNodePtr physicalNode =crd->getTopology()->getRoot();
                crd->getStreamCatalogService()->registerPhysicalStream(physicalNode,
                                                                       "MemorySource",
                                                                       "test_stream" + std::to_string(i),
                                                                       "input");
                crd->getNodeEngine()->setConfig(conf);
            }
        }
    } else if (inType == InputType::WindowMode) {
        std::cout << "windowmode source mode" << std::endl;

        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
              struct Record {
                  uint64_t id;
                  uint64_t value;
                  uint64_t payload;
                  uint64_t timestamp;
              };

              auto records = (Record*) buffer.getBuffer();

              auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count();
              for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                  records[u].id = u%1000;
                  records[u].value = u%5;
                  records[u].timestamp = ts;
              }
              //              return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf =
                NES::LambdaSourceStreamConfig::create("LambdaSource",
                                                      "test_stream" + std::to_string(i),
                                                      "input",
                                                      func,
                                                      config->getNumberOfBuffersToProduce()->getValue(),
                                                      gatheringValue,
                                                      config->getGatheringMode()->getValue());

            if (config->getScalability()->getValue() == "scale-out") {
                wrk->registerPhysicalStream(conf);
            } else {
                TopologyNodePtr physicalNode =crd->getTopology()->getRoot();
                crd->getStreamCatalogService()->registerPhysicalStream(physicalNode,
                                                                       "MemorySource",
                                                                       "test_stream" + std::to_string(i),
                                                                       "input");
                crd->getNodeEngine()->setConfig(conf);
            }
        }
    } else if (inType == InputType::YSBMode) {
        std::cout << "YSB Mode source mode" << std::endl;

        auto ysbSchema = Schema::create()
            ->addField("ysb$user_id", UINT64)
            ->addField("ysb$page_id", UINT64)
            ->addField("ysb$campaign_id", UINT64)
            ->addField("ysb$ad_type", UINT64)
            ->addField("ysb$event_type", UINT64)
            ->addField("ysb$current_ms", UINT64)
            ->addField("ysb$ip", UINT64)
            ->addField("ysb$d1", UINT64)
            ->addField("ysb$d2", UINT64)
            ->addField("ysb$d3", UINT32)
            ->addField("ysb$d4", UINT16);

        std::string input =
            R"(Schema::create()->addField("ysb$user_id", UINT64)->addField("ysb$page_id", UINT64)->addField("ysb$campaign_id", UINT64)->addField("ysb$ad_type", UINT64)->addField("ysb$event_type", UINT64)->addField("ysb$current_ms", UINT64)->addField("ysb$ip", UINT64)->addField("ysb$d1", UINT64)->addField("ysb$d2", UINT64)->addField("ysb$d3", UINT32)->addField("ysb$d4", UINT16);)";
        std::string testSchemaFileName = "ysbSchema.hpp";
        std::ofstream out(testSchemaFileName);
        out << input;
        out.close();
        NES_ASSERT(crd->getNesWorker()->registerLogicalStream("ysb", testSchemaFileName),
                   "failed to create logical stream ysb");

        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
              struct __attribute__((packed)) YsbRecord {
                  YsbRecord() = default;
                  YsbRecord(uint64_t userId,
                            uint64_t pageId,
                            uint64_t campaignId,
                            uint64_t adType,
                            uint64_t eventType,
                            uint64_t currentMs,
                            uint64_t ip)
                      : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType),
                        currentMs(currentMs), ip(ip) {}

                  uint64_t userId;
                  uint64_t pageId;
                  uint64_t campaignId;
                  uint64_t adType;
                  uint64_t eventType;
                  uint64_t currentMs;
                  uint64_t ip;

                  // placeholder to reach 78 bytes
                  uint64_t dummy1{0};
                  uint64_t dummy2{0};
                  uint32_t dummy3{0};
                  uint16_t dummy4{0};

                  YsbRecord(const YsbRecord& rhs) {
                      userId = rhs.userId;
                      pageId = rhs.pageId;
                      campaignId = rhs.campaignId;
                      adType = rhs.adType;
                      eventType = rhs.eventType;
                      currentMs = rhs.currentMs;
                      ip = rhs.ip;
                  }
                  std::string toString() const {
                      return "YsbRecord(userId=" + std::to_string(userId) + ", pageId=" + std::to_string(pageId)
                          + ", campaignId=" + std::to_string(campaignId) + ", adType=" + std::to_string(adType)
                          + ", eventType=" + std::to_string(eventType) + ", currentMs=" + std::to_string(currentMs)
                          + ", ip=" + std::to_string(ip);
                  }
              };

              auto records = (YsbRecord*) buffer.getBuffer();

              auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count();

              for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                  //                    memset(&records, 0, sizeof(YsbRecord));
                  records[u].userId = 1;
                  records[u].pageId = 0;
                  records[u].adType = 0;
                  records[u].campaignId = rand()%10000;
                  records[u].eventType = u%3;
                  records[u].currentMs = ts;
                  records[u].ip = 0x01020304;
              }

              return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf =
                NES::LambdaSourceStreamConfig::create("LambdaSource",
                                                      "YSB_phy_" + std::to_string(i),
                                                      "ysb",
                                                      func,
                                                      config->getNumberOfBuffersToProduce()->getValue(),
                                                      gatheringValue,
                                                      config->getGatheringMode()->getValue());

            if (config->getScalability()->getValue() == "scale-out") {
                wrk->registerPhysicalStream(conf);
            } else {
                TopologyNodePtr physicalNode =crd->getTopology()->getRoot();
                crd->getStreamCatalogService()->registerPhysicalStream(physicalNode,
                                                                       "MemorySource",
                                                                       "test_stream" + std::to_string(i),
                                                                       "input");
                crd->getNodeEngine()->setConfig(conf);
            }
        }
    } else if (inType == InputType::JoinMode) {
        std::cout << "joinmode source mode" << std::endl;
        for (uint64_t i = 0; i < sourceCnt; i++) {
            auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
              struct Record {
                  uint64_t id;
                  uint64_t value;
                  uint64_t timestamp;
              };

              auto records = (Record*) buffer.getBuffer();

              auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count();

              for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                  records[u].id = u%20;
                  //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                  records[u].value = u%10;
                  records[u].timestamp = ts;
              }

              return;
            };

            auto func2 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
              struct Record {
                  uint64_t id;
                  uint64_t value;
                  uint64_t timestamp;
              };

              auto records = (Record*) buffer.getBuffer();

              auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count();
              for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                  records[u].id = u%20 + 30;
                  //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                  records[u].value = u%10;
                  records[u].timestamp = ts;
              }

              return;
            };

            NES::AbstractPhysicalStreamConfigPtr conf1 =
                NES::LambdaSourceStreamConfig::create("LambdaSource",
                                                      "phy_input_1",
                                                      "input1",
                                                      func1,
                                                      config->getNumberOfBuffersToProduce()->getValue(),
                                                      gatheringValue,
                                                      config->getGatheringMode()->getValue());

            NES::AbstractPhysicalStreamConfigPtr conf2 =
                NES::LambdaSourceStreamConfig::create("LambdaSource",
                                                      "phy_input_2",
                                                      "input2",
                                                      func2,
                                                      config->getNumberOfBuffersToProduce()->getValue(),
                                                      gatheringValue,
                                                      config->getGatheringMode()->getValue());

            if (config->getScalability()->getValue() == "scale-out") {
                NES_NOT_IMPLEMENTED();
                wrk->registerLogicalStream("input1", testSchemaFileName);
                wrk->registerLogicalStream("input2", testSchemaFileName);
                wrk->registerPhysicalStream(conf1);
                wrk->registerPhysicalStream(conf2);

            } else {
                crd->getNesWorker()->registerLogicalStream("input1", testSchemaFileName);
                crd->getNesWorker()->registerLogicalStream("input2", testSchemaFileName);
                TopologyNodePtr physicalNode = crd->getTopology()->getRoot();
                crd->getStreamCatalogService()->registerPhysicalStream(physicalNode,
                                                                       "MemorySource",
                                                                       "test_stream" + std::to_string(i),
                                                                       "input");
                crd->getNodeEngine()->setConfig(conf1);

                physicalNode = crd->getTopology()->getRoot();
                crd->getStreamCatalogService()->registerPhysicalStream(physicalNode,
                                                                       "MemorySource",
                                                                       "test_stream" + std::to_string(i),
                                                                       "input");
                crd->getNodeEngine()->setConfig(conf2);
            }
        }
    } else {
        NES_ASSERT2_FMT(false, "input output mode not supported " << getInputTypeAsString(inType));
    }
}

void E2EBase::setup() {
    std::cout << "setup" << std::endl;

    portOffset += 13;
    std::string ip = "127.0.0.1";
    //uint16_t receive_port = 0;
    std::string host = "localhost";
    uint16_t publish_port = 4711;
    std::string address = ip + ":" + std::to_string(publish_port);

    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setNumWorkerThreads(numberOfWorkerThreads);
    crdConf->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
    crdConf->setNumberOfBuffersPerWorker(numberOfBuffersPerPipeline);
    crdConf->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
    crdConf->setBufferSizeInBytes(bufferSizeInBytes);
    crdConf->setRpcPort(4000 + portOffset);
    crdConf->setRestPort(8081 + portOffset);

    NES::WorkerConfigPtr wrkConf = NES::WorkerConfig::create();
    wrkConf->setNumWorkerThreads(numberOfWorkerThreads);
    wrkConf->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
    wrkConf->setNumberOfBuffersPerWorker(numberOfBuffersPerPipeline);
    wrkConf->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
    wrkConf->setBufferSizeInBytes(bufferSizeInBytes);
    wrkConf->setCoordinatorPort(crdConf->getRpcPort()->getValue());
    wrkConf->setRpcPort(crdConf->getRpcPort()->getValue() + 1);
    wrkConf->setDataPort(crdConf->getRpcPort()->getValue() + 2);
    wrkConf->setWorkerPinList(config->getWorkerPinList()->getValue());
    wrkConf->setSourcePinList(config->getSourcePinList()->getValue());
    wrkConf->setCoordinatorIp(crdConf->getRestIp()->getValue());
    wrkConf->setLocalWorkerIp(crdConf->getCoordinatorIp()->getValue());
    wrkConf->setNumaAware(true);
    std::cout << "E2EBase: Start coordinator" << std::endl;
    crd = std::make_shared<NES::NesCoordinator>(crdConf, wrkConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    //auto nodeStats = std::make_shared<NodeStats>(); //aj
    //nodeId = crd->getTopologyManagerService()->registerNode(address, 4000, 5000, 6, nodeStats, NodeType::Sensor);

    if (config->getScalability()->getValue() == "scale-out") {
        NES_NOT_IMPLEMENTED();
        std::cout << "E2EBase: Start worker 1" << std::endl;
        NES::WorkerConfigPtr wrkConf = NES::WorkerConfig::create();
        wrkConf->setCoordinatorPort(port);
        wrkConf->setRpcPort(port + 10 + portOffset);
        wrkConf->setDataPort(port + 11 + portOffset);
        std::cout << "Cport=" << port << " CsetRpcPort=" << 4000 + portOffset << " CsetRestPort=" << 8081 + portOffset
                  << " WsetRpcPort=" << port + 10 + portOffset << " WsetDataPort=" << port + 11 + portOffset
                  << std::endl;
        wrkConf->setNumWorkerThreads(numberOfWorkerThreads);
        wrkConf->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
        wrkConf->setNumberOfBuffersPerWorker(numberOfBuffersPerPipeline);
        wrkConf->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
        wrkConf->setBufferSizeInBytes(bufferSizeInBytes);
        wrk = std::make_shared<NES::NesWorker>(wrkConf, NesNodeType::Sensor);
        bool retStart1 = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        NES_ASSERT(retStart1, "retStart1");
    }

    setupSources();

    queryService = crd->getQueryService();
    queryCatalog = crd->getQueryCatalog();
}

bool E2EBase::runQuery(const QueryPlanPtr& updatedQueryPlan) {
    sleep(2);
    std::cout << "E2EBase: Submit query=" << config->getQuery()->getValue() << std::endl;
    //Query queryInp = Query::from("input").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(NullOutputSinkDescriptor::create());
    //const QueryPlanPtr queryPlan = queryInp.getQueryPlan();
    //auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    //std::cout << "Input Query Plan: " + (queryPlan)->toString() << std::endl;
    //filterPushDownRule->apply(queryPlan);
    /*
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, "BottomUp");
    */
    //queryId = queryService->validateAndQueueAddRequest(config->getQuery()->getValue(), "BottomUp");
    queryId = queryService->validateAndQueueAddRequest(config->getQuery()->getValue(), updatedQueryPlan, "BottomUp");
    //queryId = queryService->addQueryRequest(updatedPlan,"BottomUp");
    bool res = NES::TestUtils::waitForQueryToStart(queryId, queryCatalog, std::chrono::seconds(120));
    if (!res) {
        std::cout << "run does not succeed" << std::endl;
        return false;
    }

    //give the system some seconds to come to steady mode
    sleep(config->getStartupSleepIntervalInSeconds()->getValue());

    runtime = recordStatistics();

    if (runtime.count() == 0)
    {
        NES_ERROR("Engine could not be started");
        return false;
    }
    std::cout << "run time is" << runtime.count() << std::endl; //AJ
    return true;
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
    uint64_t queueSizeSum = 0;
    uint64_t availableGlobalBufferSum = 0;
    uint64_t availableFixedBufferSum = 0;

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

    for (auto& val : subPlanIdToQueueCnt) {
        queueSizeSum += val.second;
    }

    for (auto& val : subPlanIdToAvailableGlobalBufferCnt) {
        availableGlobalBufferSum += val.second;
    }

    for (auto& val : subPlanIdToAvailableFixedBufferCnt) {
        availableFixedBufferSum += val.second;
    }

#ifdef NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT
    NES_ASSERT(subPlanIdToLatencyCnt.size() == 1, "We are currently do not support multiple subplans");

    std::map<uint64_t, std::map<uint64_t, std::vector<uint64_t>>>::iterator outerMapIter;
    std::map<uint64_t, std::vector<uint64_t>>::iterator innerMapIter;
    std::vector<uint64_t>::iterator vectorIter;

    //this code loop over all subplans and summarizes latency values
    // outer loop => subplans
    // inner loop => ts as key and latency values in the vector of the maps
    // vector loop as the latency values per timestamp as a vector
    //
    for (outerMapIter = subPlanToTsToLatencyMap.begin(); outerMapIter != subPlanToTsToLatencyMap.end(); outerMapIter++) {
        for (innerMapIter = outerMapIter->second.begin(); innerMapIter != outerMapIter->second.end(); innerMapIter++) {
            uint64_t latencySum = 0;
            uint64_t latencyCnt = 0;
            //sum up the latency values
            for (vectorIter = innerMapIter->second.begin(); vectorIter != innerMapIter->second.end(); vectorIter++) {
                latencySum += *vectorIter;
                latencyCnt++;
            }
            secondsToLatencyMap[innerMapIter->first] = latencySum / latencyCnt;
            //            cout << "Subplan:" << outerMapIter->first << " ts: " << innerMapIter->first / 100 << " latencySum=" << latencySum
            //                 << " latencyCnt=" << latencyCnt << " avg latency=" << latencySum / latencyCnt << endl;
        }
    }

    auto minTs = min_element(secondsToLatencyMap.begin(), secondsToLatencyMap.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    //group ts in 100ms intervals
    for (auto& val : secondsToLatencyMap) {
        //set the ts value to start at 0
        auto& tup = hundredMsToLatencyMap[(val.first - minTs->first) / 100];
        std::get<0>(tup) = std::get<0>(tup) + val.second;
        std::get<1>(tup) = std::get<1>(tup) + 1;
    }
    stringstream sOut;

    for (auto& val : hundredMsToLatencyMap) {
        //        cout << "ts=" << val.first << " lat=" << std::get<0>(val.second) << " cnt=" << std::get<1>(val.second)
        //            << " avg=" << std::get<0>(val.second)/ std::get<1>(val.second) << endl;

        sOut << val.first << "," << std::get<0>(val.second) / std::get<1>(val.second) << endl;
    }

    std::string fileName = "Latency_W" + std::to_string(numberOfWorkerThreads) + "_Src" + std::to_string(sourceCnt) + ".csv";
    std::ofstream outFile(fileName, std::ofstream::trunc);
    outFile << sOut.str();
    outFile.close();
#endif
    std::cout << "latency sum=" << latencySum << " in ms=" << std::chrono::milliseconds(latencySum).count()
              << std::endl;
    std::cout << "queueSizeSum sum=" << queueSizeSum << std::endl;
    std::cout << "availableGlobalBufferSum sum=" << availableGlobalBufferSum << std::endl;
    std::cout << "availableFixedBufferSum sum=" << availableFixedBufferSum << std::endl;
    uint64_t runtimeInSec = std::chrono::duration_cast<std::chrono::seconds>(runtime).count();

    std::cout << "runtimeInSec=" << runtimeInSec << std::endl;

    if (bufferProcessed == 0) {
        NES_ERROR("bufferProcessed is zero thus the run is invalid");
        return out.str();
    }

    uint64_t throughputInTupsPerSec = tuplesProcessed/runtimeInSec;
    uint64_t throughputInTasksPerSec = tasksProcessed/runtimeInSec;
    uint64_t throughputInMBPerSec =
        (tuplesProcessed*defaultSchema->getSchemaSizeInBytes()/(uint64_t) runtimeInSec)/1024/1024;
    double avgLatencyInMs = (double(latencySum*1000)/(double) bufferProcessed)/1000;
    uint64_t avgQueueSize = queueSizeSum/bufferProcessed;
    uint64_t avgAvailableGlobalBuffer = availableGlobalBufferSum/bufferProcessed;
    uint64_t avgAvailableFixedBuffer = availableFixedBufferSum/bufferProcessed;

    out << bufferProcessed << "," << tasksProcessed << "," << tuplesProcessed << ","
        << tuplesProcessed*defaultSchema->getSchemaSizeInBytes() << "," << throughputInTupsPerSec << ","
        << throughputInTasksPerSec << "," << throughputInMBPerSec
        << "," << avgLatencyInMs << "," << avgQueueSize << "," << avgAvailableGlobalBuffer << ","
        << avgAvailableFixedBuffer;

    size_t tuplesPerBuffer = bufferSizeInBytes/defaultSchema->getSchemaSizeInBytes();
    std::cout.imbue(std::locale(std::cout.getloc(), new space_out));
    std::cout << "tasksProcessed=" << tasksProcessed << std::endl;
    std::cout << "bufferProcessed=" << bufferProcessed << std::endl;
    std::cout << "Input tuples=" << bufferProcessed*tuplesPerBuffer << std::endl;
    std::cout << "Output tuples=" << tuplesProcessed << std::endl;
    std::cout << "tasksProcessed=" << tasksProcessed << std::endl;
    std::cout << "tuples per sec=" << throughputInTupsPerSec << std::endl;
    std::cout << "tasks per sec=" << throughputInTasksPerSec << std::endl;
    std::cout << "avgTaskProcessingTime=" << avgLatencyInMs << std::endl;
    std::cout << "avgQueueSize=" << avgQueueSize << std::endl;
    std::cout << "avgGlobalAvailableBuffer=" << avgAvailableGlobalBuffer << std::endl;
    std::cout << "avgFixedAvailableBuffer=" << avgAvailableFixedBuffer << std::endl;
    std::cout << "runtime in sec=" << runtimeInSec << std::endl;
    std::cout << "throughput MB/sec=" << throughputInMBPerSec << std::endl;
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

