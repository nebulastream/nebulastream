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

#include <Execution/Operators/Streaming/StatisticCollection/ReservoirSample/ReservoirSampleProbe.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Util/StdInt.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>

namespace NES::Runtime::Execution::Operators {

struct LocalState : public OperatorState {
    LocalState(const Value<MemRef>& reservoirSample,
               const Value<UInt64>& observedTuples,
               const Value<UInt64>& sampleSize)
        : reservoirSample(reservoirSample), observedTuples(observedTuples), sampleSize(sampleSize) {}
    Value<MemRef> reservoirSample;
    Value<UInt64> observedTuples;
    Value<UInt64> sampleSize;
};

void* getReservoirSampleDataProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<ReservoirSampleProbeHandler*>(ptrOpHandler);
    return opHandler->getReservoirSampleData();
}

void writeCountProxy(void* ptrOpHandler, uint64_t minCount) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<ReservoirSampleProbeHandler*>(ptrOpHandler);
    opHandler->writeCount(minCount);
}

uint64_t getObservedTuplesProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<ReservoirSampleProbeHandler*>(ptrOpHandler);
    return opHandler->getObservedTuples();
}

uint64_t getSampleSizeProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<ReservoirSampleProbeHandler*>(ptrOpHandler);
    return opHandler->getSampleSize();
}

void ReservoirSampleProbe::open(ExecutionContext& executionCtx, RecordBuffer&) const {
    // We first have to get some metadata that is stored in the operator handler. We store it in the LocalState
    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<MemRef> reservoirSample = FunctionCall("getReservoirSampleDataProxy", getReservoirSampleDataProxy, operatorHandler);
    Value<UInt64> observedTuples = FunctionCall("getObservedTuplesProxy", getObservedTuplesProxy, operatorHandler);
    Value<UInt64> sampleSize = FunctionCall("getSampleSizeProxy", getSampleSizeProxy, operatorHandler);
    executionCtx.setLocalOperatorState(this, std::make_unique<LocalState>(reservoirSample, observedTuples, sampleSize));
}


void ReservoirSampleProbe::execute(ExecutionContext& ctx, Record& record) const {
    const auto operatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto localState = dynamic_cast<LocalState*>(ctx.getLocalState(this));


    Value<UInt64> counter = 0_u64;
    for (Value<UInt64> i = 0_u64; i < localState->sampleSize; i = i + 1_u64) {
        auto sampleRecord = sampleMemoryProvider->read({}, localState->reservoirSample, i);
        if (sampleRecord.read(fieldNameToCountSample) == record.read(fieldNameToCountInput)) {
            counter = counter + 1_u64;
        }
    }

    // Adapt the count to the number of observed tuples
    counter = counter * (localState->observedTuples / localState->sampleSize);
    FunctionCall("writeCountProxy", writeCountProxy, operatorHandler, counter);
}

void ReservoirSampleProbeHandler::start(PipelineExecutionContextPtr, uint32_t) {}

void ReservoirSampleProbeHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {}

void* ReservoirSampleProbeHandler::getReservoirSampleData() const { return reservoirSample->getStatisticData(); }

void ReservoirSampleProbeHandler::writeCount(uint64_t count) {
    ReservoirSampleProbeHandler::count = count;
}

Statistic::StatisticValue<> ReservoirSampleProbeHandler::getStatisticValue() {
    return Statistic::StatisticValue<>(count, reservoirSample->getStartTs(), reservoirSample->getEndTs());
}

uint64_t ReservoirSampleProbeHandler::getObservedTuples() const { return reservoirSample->getObservedTuples(); }

void ReservoirSampleProbeHandler::setNewStatistic(Statistic::StatisticPtr newStatistic) {
    ReservoirSampleProbeHandler::reservoirSample = newStatistic;
}

uint64_t ReservoirSampleProbeHandler::getSampleSize() const { return reservoirSample->as<Statistic::ReservoirSampleStatistic>()->getSampleSize(); }

ReservoirSampleProbe::ReservoirSampleProbe(MemoryProvider::MemoryProviderPtr sampleMemoryProvider,
                                           const uint64_t operatorHandlerIndex,
                                           const std::string_view fieldNameToCountInput,
                                           const std::string_view fieldNameToCountSample)
    : sampleMemoryProvider(std::move(sampleMemoryProvider)),
      operatorHandlerIndex(operatorHandlerIndex),
      fieldNameToCountInput(fieldNameToCountInput),
      fieldNameToCountSample(fieldNameToCountSample) {}

ReservoirSampleProbeHandlerPtr ReservoirSampleProbeHandler::create() {
    return std::make_shared<ReservoirSampleProbeHandler>(ReservoirSampleProbeHandler());
}
ReservoirSampleProbeHandler::ReservoirSampleProbeHandler() {}

} // namespace NES::Runtime::Execution::Operators