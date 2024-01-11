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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <Network/ExchangeProtocol.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/StdInt.hpp>
#include <Util/ThreadBarrier.hpp>
#include <filesystem>
#include <fstream>
#include <future>
#include <netdb.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace NES::Benchmark {

/**
 * This benchmark measures the performance of the output channel. This is done by spawning a zmq server. 
 */
static double BM_TestMassiveSending(uint64_t bufferSize,
                                    uint64_t buffersManaged,
                                    uint64_t numSenderThreads,
                                    uint64_t numServerThreads,
                                    uint64_t dataSize,
                                    uint32_t port,
                                    uint32_t rep) {
    std::promise<bool> completedProm;
    std::promise<uint64_t> bytesSent;
    std::promise<double> elapsedSeconds;
    double throughputRet = -1;

    uint64_t totalNumBuffer = dataSize / bufferSize;
    std::atomic<std::uint64_t> bufferReceived = 0;
    auto nesPartition = Network::NesPartition(1 + rep, 22 + rep, 333 + rep, 444 + rep);
    try {
        class ExchangeListener : public Network::ExchangeProtocolListener {

          public:
            std::atomic<std::uint64_t>& bufferReceived;
            std::promise<bool>& completedProm;

            ExchangeListener(std::atomic<std::uint64_t>& bufferReceived, std::promise<bool>& completedProm)
                : bufferReceived(bufferReceived), completedProm(completedProm) {}

            void onDataBuffer(Network::NesPartition id, Runtime::TupleBuffer& buffer) override {
                ((void) id);
                ((void) buffer);
                bufferReceived++;
            }
            void onEndOfStream(Network::Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Network::Messages::ErrorMessage) override {}

            void onChannelError(Network::Messages::ErrorMessage) override {}
            void onEvent(Network::NesPartition, Runtime::BaseEvent&) override {}
        };

        class DummyDataEmitter : public DataEmitter {
          public:
            void emitWork(Runtime::TupleBuffer&) override {}
        };

        auto partMgr = std::make_shared<Network::PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager = Network::NetworkManager::create(
            0,
            "127.0.0.1",
            port,
            Network::ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
            buffMgr,
            16,
            numServerThreads);

        auto firstBarrier = std::make_shared<NES::ThreadBarrier>(2);
        std::vector<std::thread> allThreads;
        allThreads.emplace_back([&netManager,
                                 &nesPartition,
                                 totalNumBuffer,
                                 bufferSize,
                                 &bytesSent,
                                 &elapsedSeconds,
                                 &completedProm,
                                 firstBarrier,
                                 numSenderThreads] {
            // register the incoming channel
            netManager->registerSubpartitionConsumer(nesPartition,
                                                     netManager->getServerLocation(),
                                                     std::make_shared<DummyDataEmitter>());
            firstBarrier->wait();
            auto startTime = std::chrono::steady_clock::now().time_since_epoch();
            completedProm.get_future().get();
            auto stopTime = std::chrono::steady_clock::now().time_since_epoch();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime);
            double bytes = numSenderThreads * totalNumBuffer * bufferSize;
            double throughput = (bytes * 1'000'000'000) / (elapsed.count() * 1024.0 * 1024.0);
            NES_INFO("Sent {}bytes / {}seconds = throughput {}MiB/s", bytes, (elapsed.count() / 1e9), throughput);
            bytesSent.set_value(bytes);
            elapsedSeconds.set_value(elapsed.count() / 1e9);
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        firstBarrier->wait();

        auto secondBarrier = std::make_shared<NES::ThreadBarrier>(numSenderThreads);
        for (uint64_t thread = 0; thread < numSenderThreads; ++thread) {
            allThreads.emplace_back(
                [totalNumBuffer, &completedProm, nesPartition, bufferSize, &buffMgr, &netManager, port, secondBarrier] {
                    Network::NodeLocation nodeLocation(0, "127.0.0.1", port);
                    auto senderChannel =
                        netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);
                    secondBarrier->wait();
                    if (senderChannel == nullptr) {
                        NES_DEBUG("NetworkStackTest: Error in registering DataChannel!");
                        completedProm.set_value(false);
                    } else {
                        for (uint64_t sentBuffer = 0; sentBuffer < totalNumBuffer; ++sentBuffer) {
                            auto buffer = buffMgr->getBufferBlocking();
                            for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                                buffer.getBuffer<uint64_t>()[j] = j;
                            }
                            buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                            senderChannel->sendBuffer(buffer, sizeof(uint64_t), sentBuffer + 1);
                        }
                        senderChannel->close(Runtime::QueryTerminationType::Graceful);
                        senderChannel.reset();
                    }
                    netManager->unregisterSubpartitionProducer(nesPartition);
                });
        }

        for (auto& thread : allThreads) {
            thread.join();
        }
        throughputRet = bytesSent.get_future().get() / elapsedSeconds.get_future().get();
    } catch (...) {
        NES_THROW_RUNTIME_ERROR("BenchmarkDataChannel: Error occured!");
    }
    return throughputRet;
}

}// namespace NES::Benchmark

int main(int argc, char** argv) {
    ((void) argc);
    ((void) argv);
    NES::Logger::setupLogging("NetworkChannelBenchmark.log", NES::LogLevel::LOG_INFO);

    const uint64_t buffersManaged = 32 * 1024;
    const uint64_t NUM_REPS = 10;

    // Declaring and filling all possible combinations
    std::vector<uint64_t> allBufferSizes;
    std::vector<uint64_t> allSenderThreads;
    std::vector<uint64_t> allServerThreads;
    std::vector<uint64_t> allDataSizesToBeSent;

    NES::Benchmark::Util::createRangeVectorPowerOfTwo<uint64_t>(allBufferSizes, 32 * 1024, 128 * 1024);
    NES::Benchmark::Util::createRangeVector<uint64_t>(allSenderThreads, 4, 8, 2);
    NES::Benchmark::Util::createRangeVector<uint64_t>(allServerThreads, 4, 8, 2);
    NES::Benchmark::Util::createRangeVector<uint64_t>(allDataSizesToBeSent,
                                                      1_u64 * 1024 * 1024 * 1024,
                                                      4_u64 * 1024 * 1024 * 1024,
                                                      1_u64 * 1024 * 1024 * 1024);

    std::string benchmarkName = "DataChannel";
    std::string benchmarkFolderName = "DataChannel";
    if (!std::filesystem::create_directory(benchmarkFolderName))
        throw NES::Exceptions::RuntimeException("Could not create folder " + benchmarkFolderName);

    //Writing header to csv file
    std::ofstream benchmarkFile;
    benchmarkFile.open(benchmarkFolderName + "/" + (benchmarkName) + "_results.csv", std::ios_base::app);
    benchmarkFile << "DATASIZE,NUM_SERVER_THREADS,NUM_SENDER_THREADS,BUFFERSIZE,BUFFERSMANAGED,NUM_REPS,THROUGHPUT\n";
    benchmarkFile.close();

    uint64_t totalParamCombinations =
        allDataSizesToBeSent.size() * allServerThreads.size() * allSenderThreads.size() * allBufferSizes.size();
    uint64_t curComb = 0;

    srand(time(NULL));
    int32_t min = 60000, max = 60500;
    int32_t startPort = rand() % (max - min) + min;
    std::vector<double> throughputVec;

    for (auto dataSize : allDataSizesToBeSent) {
        for (auto numServerThreads : allServerThreads) {
            for (auto numSenderThreads : allSenderThreads) {
                for (auto bufferSize : allBufferSizes) {
                    throughputVec.clear();
                    int32_t port = startPort;// + curComb;
                    for (uint64_t i = 0; i < NUM_REPS; ++i) {
                        double throughput = NES::Benchmark::BM_TestMassiveSending(bufferSize,
                                                                                  buffersManaged,
                                                                                  numSenderThreads,
                                                                                  numServerThreads,
                                                                                  dataSize,
                                                                                  port,
                                                                                  curComb);
                        throughputVec.emplace_back(throughput);
                    }
                    // Writing whole throughput vector to csv to csv file
                    for (auto throughput : throughputVec) {
                        benchmarkFile.open(benchmarkFolderName + "/" + (benchmarkName) + "_results.csv", std::ios_base::app);
                        benchmarkFile << std::to_string(dataSize) << "," << std::to_string(numServerThreads) << ","
                                      << std::to_string(numSenderThreads) << "," << std::to_string(bufferSize) << ","
                                      << std::to_string(buffersManaged) << "," << std::to_string(NUM_REPS) << ","
                                      << std::to_string(throughput) << "\n";
                        benchmarkFile.close();
                    }

                    // After a single experiment was done NUM_REPS, it is now time to calculate mean, stddev
                    double sum = 0.0;
                    double mean, stddev;

                    for (auto throughput : throughputVec) {
                        sum += throughput;
                    }
                    mean = sum / throughputVec.size();

                    sum = 0.0;
                    for (auto throughput : throughputVec) {
                        sum += pow(throughput - mean, 2);
                    }
                    stddev = sqrt(sum / throughputVec.size());

                    // Outputting current combination count and incrementing
                    std::cout << "Performed experiment [" << (++curComb) << "/" << totalParamCombinations
                              << "] with [size,serverThreads,senderThreads,buffSize,port] [" << dataSize << ","
                              << numServerThreads << "," << numSenderThreads << "," << bufferSize << "," << port
                              << "] and got [mean,stddev] of [" << mean << "," << stddev << "]\n";
                }
            }
        }
    }
    return 0;
}
