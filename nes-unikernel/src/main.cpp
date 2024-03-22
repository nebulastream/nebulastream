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
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <UnikernelExecutionPlan.hpp>

#ifndef UNIKERNEL_NUM_BUFS
#define UNIKERNEL_NUM_BUFS 1024
#endif

NES::Network::NetworkManagerPtr TheNetworkManager = nullptr;
NES::Runtime::BufferManagerPtr TheBufferManager = nullptr;
NES::Runtime::WorkerContextPtr TheWorkerContext = nullptr;

using namespace std::literals::chrono_literals;

class DummyExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
  public:
    ~DummyExchangeProtocolListener() override = default;

    void onDataBuffer(NES::Network::NesPartition, NES::Runtime::TupleBuffer&) override {}

    void onEndOfStream(NES::Network::Messages::EndOfStreamMessage) override {}

    void onServerError(NES::Network::Messages::ErrorMessage) override {}

    void onEvent(NES::Network::NesPartition, NES::Runtime::EventPtr) override {}

    void onChannelError(NES::Network::Messages::ErrorMessage) override {}
};

int main() {
    NES::Logger::setupLogging(NES::LogLevel::LOG_DEBUG);
    errno = 0;
    auto partition_manager = std::make_shared<NES::Network::PartitionManager>();
    auto exchange_listener = std::make_shared<DummyExchangeProtocolListener>();
    TheBufferManager = std::make_shared<NES::Runtime::BufferManager>(8192, UNIKERNEL_NUM_BUFS);
    TheWorkerContext = new NES::Runtime::WorkerContext(NES::Unikernel::CTConfiguration::NodeId, TheBufferManager, 1, 1);
    NES::Network::ExchangeProtocol exchange_protocol(partition_manager, exchange_listener);
    TheNetworkManager = NES::Network::NetworkManager::create(NES::Unikernel::CTConfiguration::NodeId,
                                                             NES::Unikernel::CTConfiguration::NodeIP,
                                                             NES::Unikernel::CTConfiguration::NodePort,
                                                             std::move(exchange_protocol),
                                                             TheBufferManager);
    NES::Unikernel::QueryPlan::setup();
    NES::Unikernel::QueryPlan::wait();
}