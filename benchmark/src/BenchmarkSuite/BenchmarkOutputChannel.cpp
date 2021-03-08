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

#include <Network/ExchangeProtocolListener.hpp>
#include <Network/ExchangeProtocol.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/ZmqServer.hpp>

#include <NodeEngine/TupleBuffer.hpp>
#include <future>
#include <stdint.h>

namespace NES::Benchmarking {

static double BM_TestMassiveSending(uint64_t bufferSize, uint64_t buffersManaged) {
    std::promise<bool> completedProm;
    std::promise<uint64_t> bytesSent;
    std::promise<double> elapsedSeconds;
    double throughputRet = -1;

    uint64_t totalNumBuffer = 1'000'000;
    std::atomic<std::uint64_t> bufferReceived = 0;
    auto nesPartition = Network::NesPartition(1, 22, 333, 444);
    try {
        class ExchangeListener : public Network::ExchangeProtocolListener {

          public:
            std::promise<bool>& completedProm;
            std::atomic<std::uint64_t>& bufferReceived;

            ExchangeListener(std::atomic<std::uint64_t>& bufferReceived, std::promise<bool>& completedProm)
                : bufferReceived(bufferReceived), completedProm(completedProm) {}

            void onDataBuffer(Network::NesPartition id, NodeEngine::TupleBuffer& buffer) override {
                auto bufferContent = buffer.getBuffer<uint64_t>();
                bufferReceived++;
            }
            void onEndOfStream(Network::Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Network::Messages::ErrorMessage) override {}

            void onChannelError(Network::Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<Network::PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);

        auto netManager = Network::NetworkManager::create(
            "127.0.0.1", 31337,
            Network::ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)), buffMgr);

        std::thread t([&netManager, &nesPartition, &completedProm, totalNumBuffer, bufferSize, &bytesSent, &elapsedSeconds] {
            // register the incoming channel
            netManager->registerSubpartitionConsumer(nesPartition);
            auto startTime = std::chrono::steady_clock::now().time_since_epoch();
            completedProm.get_future().get();
            auto stopTime = std::chrono::steady_clock::now().time_since_epoch();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime);
            double bytes = totalNumBuffer * bufferSize;
            double throughput = (bytes * 1'000'000'000) / (elapsed.count() * 1024.0 * 1024.0);
            std::cout << "Sent " << bytes << " bytes / " << (elapsed.count()/1e9) << " elapsed seconds = throughput " << throughput << " MiB/s" << std::endl;
            bytesSent.set_value(bytes);
            elapsedSeconds.set_value(elapsed.count()/1e9);
        });

        Network::NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager->registerSubpartitionProducer(nodeLocation, nesPartition, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_DEBUG("NetworkStackTest: Error in registering OutputChannel!");
            completedProm.set_value(false);
        } else {
            for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                auto buffer = buffMgr->getBufferBlocking();
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    buffer.getBuffer<uint64_t>()[j] = j;
                }
                buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                senderChannel->sendBuffer(buffer, sizeof(uint64_t));
            }
            senderChannel.reset();
        }
        t.join();

        throughputRet = bytesSent.get_future().get() / elapsedSeconds.get_future().get();

    } catch (...) {
        NES_THROW_RUNTIME_ERROR("BenchmarkOutputChannel: Error occured!");
    }

    return throughputRet;
}
}

int main(int argc, char** argv){
    ((void) argc);
    ((void) argv);

    const uint64_t buffersManaged = 8 * 1024;
    const uint64_t bufferSize = 32 * 1024;

    const uint64_t NUM_REPS = 10;

    double throughputSum = 0.0;
    for (uint64_t i = 0; i < NUM_REPS; ++i) {
        throughputSum += NES::Benchmarking::BM_TestMassiveSending(bufferSize, buffersManaged);
    }

    std::cout << "Avg throughput = " << (throughputSum / NUM_REPS) << std::endl;


    return 0;
}