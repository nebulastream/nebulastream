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
#include "Pipeline.h"
#include "UnikernelStage.h"
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <State/StateManager.hpp>
#include <TupleBufferProxyFunctions.hpp>
#include <Utils.hpp>
#include <argumentum/argparse.h>
#include <string>

NES::Network::NetworkManagerPtr the_networkmanager = nullptr;
NES::Runtime::BufferManagerPtr the_buffermanager = nullptr;
NES::Runtime::WorkerContextPtr the_workerContext = nullptr;

using namespace std::literals::chrono_literals;

class DummyExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
  public:
    ~DummyExchangeProtocolListener() override = default;

    void onDataBuffer(NES::Network::NesPartition, NES::Runtime::TupleBuffer&) override {}

    void onEndOfStream(NES::Network::Messages::EndOfStreamMessage) override { NES_INFO("Party is Over!"); }

    void onServerError(NES::Network::Messages::ErrorMessage) override {}

    void onEvent(NES::Network::NesPartition, NES::Runtime::BaseEvent&) override {}

    void onChannelError(NES::Network::Messages::ErrorMessage) override {}
};

extern "C" uint64_t getWorkerIdProxy(void* workerContext) {
    auto* wc = (NES::Runtime::WorkerContext*) workerContext;
    return wc->getId();
}

extern "C" void* allocateBufferProxy(void* worker_context_ptr) {
    if (worker_context_ptr == nullptr) {
        NES_THROW_RUNTIME_ERROR("worker context should not be null");
    }
    auto wc = static_cast<NES::Runtime::WorkerContext*>(worker_context_ptr);
    // We allocate a new tuple buffer for the runtime.
    // As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
    // This increases the reference counter in the buffer.
    // When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
    auto buffer = wc->getBufferProvider()->getBufferBlocking();
    auto* tb = new NES::Runtime::TupleBuffer(buffer);
    return tb;
}

extern "C" [[maybe_unused]] void emitBufferProxy(void* worker_context_ptr, void* pc_ptr, void* tupleBuffer) {
    NES_TRACE("Emit Buffer Proxy: {} {} {}", worker_context_ptr, pc_ptr, tupleBuffer);
    auto* tb = (NES::Runtime::TupleBuffer*) tupleBuffer;
    auto pipeline_context = static_cast<NES::Unikernel::UnikernelPipelineExecutionContextBase*>(pc_ptr);
    if (tb->getNumberOfTuples() != 0) {
        pipeline_context->emit(*tb);
    }
    tb->release();
}
int main() {
    NES::Logger::setupLogging(NES::LogLevel::LOG_TRACE);
    auto partition_manager = std::make_shared<NES::Network::PartitionManager>();
    auto exchange_listener = std::make_shared<DummyExchangeProtocolListener>();
    the_buffermanager = std::make_shared<NES::Runtime::BufferManager>();
    the_buffermanager->createFixedSizeBufferPool(128);
    the_workerContext =
        std::make_shared<NES::Runtime::WorkerContext>(NES::Unikernel::CTConfiguration::NodeID, the_buffermanager, 1, 1);
    NES::Network::ExchangeProtocol exchange_protocol(partition_manager, exchange_listener);
    the_networkmanager = NES::Network::NetworkManager::create(NES::Unikernel::CTConfiguration::NodeID,
                                                              NES::Unikernel::CTConfiguration::NodeIP,
                                                              NES::Unikernel::CTConfiguration::NodePort,
                                                              std::move(exchange_protocol),
                                                              the_buffermanager);

    auto state_manager = std::make_shared<NES::Runtime::StateManager>(NES::Unikernel::CTConfiguration::NodeID);

    NES::Unikernel::QueryPlan qp;
    qp.setup();
    qp.execute();

    sleep(1000);
    qp.stop();
}