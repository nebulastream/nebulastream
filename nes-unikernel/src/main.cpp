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
#include <API/Schema.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/WorkerContext.hpp>
#include <UnikernelExecutionPlan.hpp>

NES::Network::NetworkManagerPtr TheNetworkManager = nullptr;
NES::Runtime::BufferManagerPtr TheBufferManager = nullptr;
NES::Runtime::WorkerContextPtr TheWorkerContext = nullptr;

using namespace std::literals::chrono_literals;

std::mutex m;
std::condition_variable stop_condition;
bool should_exit = false;

class DummyExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
  public:
    ~DummyExchangeProtocolListener() override = default;

    void onDataBuffer(NES::Network::NesPartition, NES::Runtime::TupleBuffer&) override {}

    void onEndOfStream(NES::Network::Messages::EndOfStreamMessage) override {
        {
            std::unique_lock lock(m);
            should_exit = true;
        }
        stop_condition.notify_all();
    }

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
    auto pipeline_context = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pc_ptr);
    if (tb->getNumberOfTuples() != 0) {
        pipeline_context->emit(*tb);
    }
    tb->release();
}

int main() {
    NES::Logger::setupLogging(static_cast<NES::LogLevel>(NES_COMPILE_TIME_LOG_LEVEL));
    errno = 0;
    auto partition_manager = std::make_shared<NES::Network::PartitionManager>();
    auto exchange_listener = std::make_shared<DummyExchangeProtocolListener>();
    TheBufferManager = std::make_shared<NES::Runtime::BufferManager>();
    TheBufferManager->createFixedSizeBufferPool(128);
    TheWorkerContext = new NES::Runtime::WorkerContext(NES::Unikernel::CTConfiguration::NodeID, TheBufferManager, 1, 1);
    NES::Network::ExchangeProtocol exchange_protocol(partition_manager, exchange_listener);
    TheNetworkManager = NES::Network::NetworkManager::create(NES::Unikernel::CTConfiguration::NodeID,
                                                             NES::Unikernel::CTConfiguration::NodeIP,
                                                             NES::Unikernel::CTConfiguration::NodePort,
                                                             std::move(exchange_protocol),
                                                             TheBufferManager);
    NES::Unikernel::QueryPlan::setup();
    NES::Unikernel::QueryPlan::wait();
}