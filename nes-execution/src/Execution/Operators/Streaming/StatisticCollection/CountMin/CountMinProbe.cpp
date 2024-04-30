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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinProbe.hpp>
#include <Statistics/Synopses/CountMinStatistic.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <random>

namespace NES::Runtime::Execution::Operators {

struct LocalState : public OperatorState {
    LocalState(const Value<MemRef>& countMinRef,
               const Value<MemRef>& h3SeedsMemRef,
               const Value<UInt64>& sizeOfOneRowInBytesSeeds,
               const Value<UInt64>& sizeOfOneCounterInBytes,
               const Value<UInt64>& depth,
               const Value<UInt64>& width)
        : countMinRef(countMinRef), h3SeedsMemRef(h3SeedsMemRef), sizeOfOneRowInBytesSeeds(sizeOfOneRowInBytesSeeds),
          sizeOfOneCounterInBytes(sizeOfOneCounterInBytes), depth(depth), width(width) {}
    Value<MemRef> countMinRef;
    Value<MemRef> h3SeedsMemRef;
    Value<UInt64> sizeOfOneRowInBytesSeeds;
    Value<UInt64> sizeOfOneCounterInBytes;
    Value<UInt64> depth;
    Value<UInt64> width;
};

void* getCountMinStatisticDataProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinProbeHandler*>(ptrOpHandler);
    return opHandler->getCountMinStatisticData();
}

void* getH3SeedsProbeProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinProbeHandler*>(ptrOpHandler);

    // This const_cast is fine, as we do not change the values
    return reinterpret_cast<int8_t*>(const_cast<uint64_t*>(opHandler->getH3Seeds().data()));
}

void writeMinCountProxy(void* ptrOpHandler, uint64_t minCount) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinProbeHandler*>(ptrOpHandler);
    opHandler->writeMinCount(minCount);
}

uint64_t getDepthProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinProbeHandler*>(ptrOpHandler);
    return opHandler->getDepth();
}

uint64_t getWidthProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinProbeHandler*>(ptrOpHandler);
    return opHandler->getWidth();
}

uint64_t getSizeOfOneRowInBytesSeedsProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinProbeHandler*>(ptrOpHandler);
    return opHandler->getSizeOfOneRowInBytesSeeds();
}

uint64_t getSizeOfOneCounterInBytesProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinProbeHandler*>(ptrOpHandler);
    return opHandler->getSizeOfOneCounterInBytes();
}

void CountMinProbe::open(ExecutionContext& executionCtx, RecordBuffer&) const {
    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<MemRef> countMinRef = FunctionCall("getCountMinStatisticDataProxy", getCountMinStatisticDataProxy, operatorHandler);
    Value<MemRef> h3SeedsMemRef = FunctionCall("getH3SeedsProbeProxy", getH3SeedsProbeProxy, operatorHandler);
    Value<UInt64> sizeOfOneRowInBytesSeeds = FunctionCall("getSizeOfOneRowInBytesSeedsProxy", getSizeOfOneRowInBytesSeedsProxy, operatorHandler);
    Value<UInt64> sizeOfOneCounterInBytes = FunctionCall("getSizeOfOneCounterInBytesProxy", getSizeOfOneCounterInBytesProxy, operatorHandler);
    Value<UInt64> depth = FunctionCall("getDepthProxy", getDepthProxy, operatorHandler);
    Value<UInt64> width = FunctionCall("getWidthProxy", getWidthProxy, operatorHandler);
    executionCtx.setLocalOperatorState(this, std::make_unique<LocalState>(countMinRef, h3SeedsMemRef, sizeOfOneRowInBytesSeeds, sizeOfOneCounterInBytes, depth, width));
}

void CountMinProbe::execute(ExecutionContext& ctx, Record& record) const {
    using namespace Nautilus;
    auto localState = dynamic_cast<LocalState*>(ctx.getLocalState(this));

    // 2. Performing the probe by hashing the value and taking the minimum across all rows
    Value<UInt64> minCount = std::numeric_limits<uint64_t>::max();
    for (Value<UInt64> row = 0_u64; row < localState->depth; row = row + 1_u64) {
        // 2.1 We calculate a MemRef to the first h3Seeds of the current row
        Value<UInt64> rowOffSetSeeds((row * localState->sizeOfOneRowInBytesSeeds).as<UInt64>());
        Value<MemRef> h3SeedsThisRow = (localState->h3SeedsMemRef + rowOffSetSeeds).as<MemRef>();

        // 2.2. Hashing the current value
        auto valToTrack = record.read(keyFieldName);
        Value<UInt64> calcHash = h3HashFunction->calculateWithState(valToTrack, h3SeedsThisRow);
        Value<UInt64> col = (calcHash % localState->width).as<UInt64>();

        // 2.3. Getting the value at the hashed position
        Value<UInt64> rowOffSetCounter((row * localState->sizeOfOneCounterInBytes * localState->width).as<UInt64>());
        Value<UInt64> colOffSetCounter((col * localState->sizeOfOneCounterInBytes).as<UInt64>());
        Value<MemRef> valueRef = (localState->countMinRef + rowOffSetCounter + colOffSetCounter).as<MemRef>();
        Value<UInt64> count = valueRef.load<UInt64>();

        // 2.4. If the count is smaller, store it in the minCount
        if (count < minCount) {
            minCount = count;
        }
    }

    // 3. Emitting the minimum count by writing it to the operator handler
    auto operatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    FunctionCall("writeMinCountProxy", writeMinCountProxy, operatorHandler, minCount);
}

void CountMinProbeHandler::start(PipelineExecutionContextPtr, uint32_t) {}

void CountMinProbeHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {}

void* CountMinProbeHandler::getCountMinStatisticData() const { return countMinStatistic->getStatisticData(); }

const std::vector<uint64_t>& CountMinProbeHandler::getH3Seeds() const { return h3Seeds; }

void CountMinProbeHandler::writeMinCount(uint64_t minCount) {
    CountMinProbeHandler::minCount = minCount;
}

Statistic::StatisticValue<> CountMinProbeHandler::getStatisticValue() {
    return Statistic::StatisticValue<>(minCount, countMinStatistic->getStartTs(), countMinStatistic->getEndTs());
}

void CountMinProbeHandler::setNewStatistic(Statistic::StatisticPtr newStatistic, const uint64_t numberOfBitsInKey) {
    // Creating here the H3-Seeds with a custom seed, allowing us to not have to send the seed with the statistics
    std::random_device rd;
    std::mt19937 gen(H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;
    for (auto row = 0UL; row < newStatistic->as<Statistic::CountMinStatistic>()->getDepth(); ++row) {
        for (auto keyBit = 0UL; keyBit < numberOfBitsInKey; ++keyBit) {
            h3Seeds.emplace_back(distribution(gen));
        }
    }

    CountMinProbeHandler::sizeOfOneRowInBytesSeeds = ((numberOfBitsInKey * NUMBER_OF_BITS_IN_HASH_VALUE) / 8);
    CountMinProbeHandler::sizeOfOneCounterInBytes = sizeof(uint64_t);
    CountMinProbeHandler::countMinStatistic = newStatistic;
}

CountMinProbeHandlerPtr CountMinProbeHandler::create() {
    return std::make_shared<CountMinProbeHandler>(CountMinProbeHandler());
}

CountMinProbeHandler::CountMinProbeHandler() {}

uint64_t CountMinProbeHandler::getSizeOfOneRowInBytesSeeds() const { return sizeOfOneRowInBytesSeeds; }
uint64_t CountMinProbeHandler::getSizeOfOneCounterInBytes() const { return sizeOfOneCounterInBytes; }
uint64_t CountMinProbeHandler::getDepth() const { return countMinStatistic->as<Statistic::CountMinStatistic>()->getDepth(); }
uint64_t CountMinProbeHandler::getWidth() const { return countMinStatistic->as<Statistic::CountMinStatistic>()->getWidth(); }

CountMinProbe::CountMinProbe(const uint64_t operatorHandlerIndex,
                             const std::string_view keyFieldName,
                             std::unique_ptr<Nautilus::Interface::HashFunction> h3HashFunction)
    : operatorHandlerIndex(operatorHandlerIndex), keyFieldName(keyFieldName),
      h3HashFunction(std::move(h3HashFunction)) {}
} // namespace NES::Runtime::Execution::Operators