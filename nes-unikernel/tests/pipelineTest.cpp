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
#include <UnikernelExecutionPlan.hpp>
#include <UnikernelStage.hpp>

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
static bool received_from_1 = false;
static bool received_from_2 = false;
template<>
void Stage<4>::execute(NES::Unikernel::UnikernelPipelineExecutionContext& context,
                       NES::Runtime::WorkerContext*,
                       NES::Runtime::TupleBuffer& buffer) {
    if (buffer.getOriginId() == 1) {
        NES_ASSERT(!received_from_1, "Expected only 1 Buffer from 1");
        NES_INFO("Received from 1");
        received_from_1 = true;
    }
    if (buffer.getOriginId() == 2) {
        NES_ASSERT(!received_from_2, "Expected only 1 Buffer from 1");
        NES_INFO("Received from 2");
        received_from_2 = true;
    }

    if (received_from_1 && received_from_2) {
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
struct TestSinkImpl {
    OperatorId operatorId;
    void writeData(NES::Runtime::TupleBuffer&, NES::Runtime::WorkerContext&) { NES_INFO("Sink Received Data"); }
};
struct TestSink {
    using SinkType = TestSinkImpl;
};

struct TestSource {
    static std::map<NES::OriginId, TestSource*> sources;
    virtual void emit(NES::Runtime::TupleBuffer& tb) = 0;
    virtual ~TestSource() = default;
};
std::map<NES::OriginId, TestSource*> TestSource::sources = {};
template<typename Config>
struct TestSourceImpl : public TestSource {
    size_t seqNumber = 0;
    UnikernelPipelineExecutionContext next;

    explicit TestSourceImpl(UnikernelPipelineExecutionContext next) : next(std::move(next)) {
        TestSource::sources[Config::OriginId] = this;
    }

    void emit(NES::Runtime::TupleBuffer& tb) override {
        tb.setOriginId(Config::OriginId);
        tb.setSequenceNumber(seqNumber);
        NES_INFO("Source {} Emitted TupleBuffer: {}", Config::OriginId, seqNumber);
        seqNumber++;
        next.emit(tb);
    }
};
struct TestSource1 {
    using SourceType = TestSourceImpl<TestSource1>;
    static constexpr NES::NodeId UpstreamNodeID = 1;
    static constexpr NES::OriginId OriginId = 1;
};

struct TestSource2 {
    using SourceType = TestSourceImpl<TestSource2>;
    static constexpr NES::NodeId UpstreamNodeID = 2;
    static constexpr NES::OriginId OriginId = 2;
};

using Sink = UnikernelSink<TestSink>;
using Source1 = UnikernelSource<TestSource1>;
using Source2 = UnikernelSource<TestSource2>;
using Join = PipelineJoin<Stage<4>, Pipeline<Stage<5>, Source1>, Pipeline<Stage<6>, Source2>>;
using SubQueryPlan = SubQuery<Sink, Join>;
using QueryPlan = UnikernelExecutionPlan<SubQueryPlan>;

}// namespace NES::Unikernel

NES::Runtime::WorkerContextPtr TheWorkerContext = nullptr;
int main() {
    NES::Logger::setupLogging(NES::LogLevel::LOG_DEBUG);
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>();
    bufferManager->createFixedSizeBufferPool(128);
    TheWorkerContext = new NES::Runtime::WorkerContext(0, bufferManager, 1, 1);
    NES::Unikernel::QueryPlan::setup();
    auto& source1 = *NES::Unikernel::TestSource::sources[1];
    auto& source2 = *NES::Unikernel::TestSource::sources[2];
    auto buffer = bufferManager->getBufferBlocking();
    source1.emit(buffer);
    auto buffer2 = bufferManager->getBufferBlocking();
    source2.emit(buffer2);

    return 0;
}
