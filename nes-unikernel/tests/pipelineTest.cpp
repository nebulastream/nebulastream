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
#include "../../nes-benchmark/include/DataProvider/TupleBufferHolder.hpp"

#include <UnikernelExecutionPlan.hpp>
#include <UnikernelStage.hpp>
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <variant>

namespace NES::Benchmark::DataProvision {
class TupleBufferHolder;
}
namespace NES::Unikernel {
#define CREATE_SIMPLE_STAGE(STAGE_ID)                                                                                            \
    template<>                                                                                                                   \
    NES::Runtime::Execution::OperatorHandler* Stage<STAGE_ID>::getOperatorHandler(int) {                                         \
        NES_THROW_RUNTIME_ERROR("getOperatorHandler should not have been called");                                               \
        return nullptr;                                                                                                          \
    }                                                                                                                            \
    template<>                                                                                                                   \
    void Stage<STAGE_ID>::setup(UnikernelPipelineExecutionContext&, NES::Runtime::WorkerContext*) {                              \
        NES_INFO("Setup for stage {} was called", STAGE_ID);                                                                     \
    }                                                                                                                            \
    template<>                                                                                                                   \
    void Stage<STAGE_ID>::terminate(UnikernelPipelineExecutionContext&, NES::Runtime::WorkerContext*) {                          \
        NES_INFO("Terminate for stage {} was called", STAGE_ID);                                                                 \
    }

CREATE_SIMPLE_STAGE(4)
static bool stage_4_received_from_1 = false;
static bool stage_4_received_from_2 = false;
template<>
void Stage<4>::execute(NES::Unikernel::UnikernelPipelineExecutionContext& context,
                       NES::Runtime::WorkerContext*,
                       NES::Runtime::TupleBuffer& buffer) {
    if (buffer.getOriginId() == 1) {
        NES_ASSERT(!stage_4_received_from_1, "Expected only 1 Buffer from 1");
        NES_INFO("Received from 1");
        stage_4_received_from_1 = true;
    }
    if (buffer.getOriginId() == 2) {
        NES_ASSERT(!stage_4_received_from_2, "Expected only 1 Buffer from 1");
        NES_INFO("Received from 2");
        stage_4_received_from_2 = true;
    }

    if (stage_4_received_from_1 && stage_4_received_from_2) {
        buffer.setOriginId(3);
        NES_INFO("Join Done");
        context.emit(buffer);
    }
}

CREATE_SIMPLE_STAGE(5)
template<>
void Stage<5>::execute(NES::Unikernel::UnikernelPipelineExecutionContext& context,
                       NES::Runtime::WorkerContext*,
                       NES::Runtime::TupleBuffer& buffer) {
    NES_INFO("Stage 5 Execute");
    context.emit(buffer);
}
CREATE_SIMPLE_STAGE(6)
template<>
void Stage<6>::execute(NES::Unikernel::UnikernelPipelineExecutionContext& context,
                       NES::Runtime::WorkerContext*,
                       NES::Runtime::TupleBuffer& buffer) {
    NES_INFO("Stage 6 Execute");
    context.emit(buffer);
}

CREATE_SIMPLE_STAGE(7)
template<>
void Stage<7>::execute(NES::Unikernel::UnikernelPipelineExecutionContext& context,
                       NES::Runtime::WorkerContext*,
                       NES::Runtime::TupleBuffer& buffer) {
    NES_INFO("Stage 7 Execute");
    context.emit(buffer);
}

CREATE_SIMPLE_STAGE(8)
static bool stage_8_received_from_1 = false;
static bool stage_8_received_from_2 = false;
template<>
void Stage<8>::execute(NES::Unikernel::UnikernelPipelineExecutionContext& context,
                       NES::Runtime::WorkerContext*,
                       NES::Runtime::TupleBuffer& buffer) {
    if (buffer.getOriginId() == 1) {
        NES_ASSERT(!stage_8_received_from_1, "Expected only 1 Buffer from 1");
        NES_INFO("Stage 8 Received from 1");
        stage_8_received_from_1 = true;
    }
    if (buffer.getOriginId() == 2) {
        NES_ASSERT(!stage_8_received_from_2, "Expected only 1 Buffer from 1");
        NES_INFO("Stage 8 Received from 2");
        stage_8_received_from_2 = true;
    }

    if (stage_8_received_from_1 && stage_8_received_from_2) {
        buffer.setOriginId(3);
        NES_INFO("Stage 8 Join Done");
        context.emit(buffer);
    }
}

struct TestSinkImpl {
    OperatorId operatorId;
    void writeData(NES::Runtime::TupleBuffer&, NES::Runtime::WorkerContext&) { NES_INFO("Sink Received Data"); }
    void shutdown() { NES_INFO("Sink was shutdown"); }
};
struct TestSink {
    using SinkType = TestSinkImpl;
};

// helper type for the visitor #4
template<class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
// explicit deduction guide (not needed as of C++20)
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

template<typename Config>
struct TestSourceImpl final {
    using QueueValueType = std::variant<Benchmark::DataProvision::TupleBufferHolder, bool>;
    using SPSCQueue = folly::DynamicBoundedQueue<QueueValueType, true, true, true>;
    using BufferType = SchemaBuffer<typename Config::Schema, 8192>;
    constexpr static NES::SourceType SourceType = NES::SourceType::TEST_SOURCE;
    constexpr static bool NeedsBuffer = Config::CopyBuffer;
    SPSCQueue queue{100};
    std::atomic_bool opened = false;
    std::atomic_bool closed = false;
    std::atomic_bool stop_request = false;

    TestSourceImpl();

    void emit(NES::Runtime::TupleBuffer& tb) { queue.enqueue(tb); }

    void eof() { queue.enqueue(true); }

    bool open() {
        NES_ASSERT(!opened.exchange(true), "Open called multiple times");
        return true;
    }

    bool close(Runtime::QueryTerminationType) {
        NES_ASSERT(!closed.exchange(true), "Close called multiple times");
        return true;
    }

    std::optional<NES::Runtime::TupleBuffer> receiveData(const std::stop_token& stoken, BufferType buffer) {
        QueueValueType item;
        while (!(queue.try_dequeue_for(item, 20ms) || stoken.stop_requested())) {
        }

        if (stoken.stop_requested()) {
            NES_ASSERT(!stop_request.exchange(true), "stop was requested multiple times");
            return std::nullopt;
        }

        if constexpr (Config::CopyBuffer) {
            return std::visit(
                overloaded{
                    [&buffer](const Benchmark::DataProvision::TupleBufferHolder& tb) -> std::optional<NES::Runtime::TupleBuffer> {
                        NES_ASSERT(tb.bufferToHold.getBufferSize() == 8192, "Expected Buffer Size");
                        std::memcpy(buffer.getBuffer().getBuffer(), tb.bufferToHold.getBuffer(), 8192);
                        return buffer.getBuffer();
                    },
                    [](const bool& /*bool*/) -> std::optional<NES::Runtime::TupleBuffer> {
                        return std::nullopt;
                    }},
                item);
        } else {
            return std::visit(
                overloaded{[](const Benchmark::DataProvision::TupleBufferHolder& tb) -> std::optional<NES::Runtime::TupleBuffer> {
                               return tb.bufferToHold;
                           },
                           [](const bool& /*bool*/) -> std::optional<NES::Runtime::TupleBuffer> {
                               return std::nullopt;
                           }},
                item);
        }
    }
};

struct TestSourceConfig1 {
    constexpr static bool CopyBuffer = true;
    constexpr static size_t OriginId = 1;

    constexpr static size_t LocalBuffers = 100;
    constexpr static size_t OperatorId = 1;
    using Schema = Schema<Field<INT32>>;
    using SourceType = TestSourceImpl<TestSourceConfig1>;
};

struct TestSourceConfig2 {
    constexpr static bool CopyBuffer = false;
    constexpr static size_t OriginId = 0;

    constexpr static size_t LocalBuffers = 100;
    constexpr static size_t OperatorId = 2;
    using Schema = Schema<Field<INT32>>;
    using SourceType = TestSourceImpl<TestSourceConfig2>;
};

struct TestSourceConfig3 {
    constexpr static bool CopyBuffer = false;
    constexpr static size_t OriginId = 0;

    constexpr static size_t LocalBuffers = 100;
    constexpr static size_t OperatorId = 3;
    using Schema = Schema<Field<INT32>>;
    using SourceType = TestSourceImpl<TestSourceConfig3>;
};

static_assert(DataSourceConfig<TestSourceConfig1>, "TestSourceConfig does not implement DataSourceConfig");
static_assert(DataSourceConfig<TestSourceConfig2>, "TestSourceConfig does not implement DataSourceConfig");
static_assert(DataSourceConfig<TestSourceConfig3>, "TestSourceConfig does not implement DataSourceConfig");

static_assert(DataSourceImpl<TestSourceImpl<TestSourceConfig1>, TestSourceConfig1>,
              "Test source does not implement DataSourceImpl");
static_assert(DataSourceImpl<TestSourceImpl<TestSourceConfig2>, TestSourceConfig2>,
              "Test source does not implement DataSourceImpl");
static_assert(DataSourceImpl<TestSourceImpl<TestSourceConfig3>, TestSourceConfig2>,
              "Test source does not implement DataSourceImpl");

template<typename Config>
TestSourceImpl<Config>* SourceHandle = nullptr;

template<typename Config>
TestSourceImpl<Config>::TestSourceImpl() {
    SourceHandle<Config> = this;
}

using Sink = UnikernelSink<TestSink>;
using Source1 = UnikernelSource<TestSourceConfig1>;
using Source2 = UnikernelSource<TestSourceConfig2>;
using Source3 = UnikernelSource<TestSourceConfig3>;
using FirstJoin = PipelineJoin<Stage<4>, Pipeline<Stage<5>, Source1>, Pipeline<Stage<6>, Source2>>;
using SecondJoin = PipelineJoin<Stage<8>, Pipeline<Stage<7>, Source3>, FirstJoin>;
using SubQueryPlan = SubQuery<Sink, SecondJoin>;
using QueryPlan = UnikernelExecutionPlan<SubQueryPlan>;

}// namespace NES::Unikernel

NES::Runtime::WorkerContextPtr TheWorkerContext = nullptr;
NES::Network::NetworkManagerPtr TheNetworkManager = nullptr;
NES::Runtime::BufferManagerPtr TheBufferManager = nullptr;
int main() {
    NES::Logger::setupLogging(NES::LogLevel::LOG_DEBUG);
    TheBufferManager = std::make_shared<NES::Runtime::BufferManager>();
    TheBufferManager->createFixedSizeBufferPool(128);
    TheWorkerContext = new NES::Runtime::WorkerContext(0, TheBufferManager, 1, 1);
    NES::Unikernel::QueryPlan::setup();
    auto& source1 = *NES::Unikernel::SourceHandle<NES::Unikernel::TestSourceConfig1>;
    auto& source2 = *NES::Unikernel::SourceHandle<NES::Unikernel::TestSourceConfig2>;
    auto& source3 = *NES::Unikernel::SourceHandle<NES::Unikernel::TestSourceConfig3>;

    auto t1 = std::jthread([&source1]() {
        auto buffer = TheBufferManager->getBufferBlocking();
        source1.emit(buffer);
        source1.eof();
    });

    auto t2 = std::jthread([&source2]() {
        auto buffer = TheBufferManager->getBufferBlocking();
        buffer.setOriginId(2);
        source2.emit(buffer);
        source2.eof();
    });

    auto t3 = std::jthread([&source3]() {
        auto buffer = TheBufferManager->getBufferBlocking();
        buffer.setOriginId(3);
        source3.emit(buffer);
        source3.eof();
    });

    NES::Unikernel::QueryPlan::wait();
    return 0;
}
