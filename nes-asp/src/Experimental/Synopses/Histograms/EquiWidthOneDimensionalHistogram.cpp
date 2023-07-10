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

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Experimental/Synopses/Histograms/EquiWidthOneDimensionalHistogram.hpp>
#include <Experimental/Synopses/Histograms/EquiWidthOneDimensionalHistogramOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::ASP {

void* getBinsRefProxy(void* opHandlerPtr) {
    NES_ASSERT2_FMT(opHandlerPtr != nullptr, "opHandlerPtr can not be null");
    auto* opHandler = static_cast<EquiWidthOneDimensionalHistogramOperatorHandler*>(opHandlerPtr);
    return opHandler->getBinsRef();
}

void setupOpHandlerProxy(void* opHandlerPtr, uint64_t entrySize, uint64_t numberOfBins) {
    NES_ASSERT2_FMT(opHandlerPtr != nullptr, "opHandlerPtr can not be null");
    auto* opHandler = static_cast<EquiWidthOneDimensionalHistogramOperatorHandler*>(opHandlerPtr);
    opHandler->setup(entrySize, numberOfBins);
}


void EquiWidthOneDimensionalHistogram::addToSynopsis(const uint64_t, Runtime::Execution::ExecutionContext&,
                                                     Nautilus::Record record,
                                                     const Runtime::Execution::Operators::OperatorState *pState) {

    // Retrieving the reference to the bins
    NES_ASSERT2_FMT(pState != nullptr, "Local state was null, but we expected it not to be null!");
    auto localState = dynamic_cast<const LocalBinsOperatorState*>(pState);
    auto& bins = localState->bins;

    auto binDimensionPos = ((readKeyExpression->execute(record) - minValue) / binWidth);
    aggregationFunction->lift(bins[0][binDimensionPos], record);
}

void EquiWidthOneDimensionalHistogram::getApproximateRecord(const uint64_t handlerIndex,
                                                            Runtime::Execution::ExecutionContext& ctx,
                                                            const Nautilus::Value<>& key,
                                                            Nautilus::Record& outputRecord) {
    using namespace Runtime::Execution;

    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    auto binsMemRef = Nautilus::FunctionCall("getBinsRefProxy", getBinsRefProxy, opHandler);
    auto bins = Nautilus::Interface::Fixed2DArrayRef(binsMemRef, entrySize, numberOfBins);

    // Calculating the bin position and then lowering the approximation
    auto binDimensionPos = ((key - minValue) / binWidth);
    aggregationFunction->lower(bins[0][binDimensionPos], outputRecord);

    // Writing the key, lower and upper value of the current bin
    auto lowerBinBound = binDimensionPos * binWidth;
    auto upperBinBound = (binDimensionPos + 1) * binWidth;
    outputRecord.write(lowerBinBoundString, lowerBinBound);
    outputRecord.write(upperBinBoundString, upperBinBound);
    outputRecord.write(fieldNameKey, key);

}

void EquiWidthOneDimensionalHistogram::setup(const uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) {
    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    Nautilus::FunctionCall("setupOpHandlerProxy", setupOpHandlerProxy, opHandler,
                           Nautilus::Value<Nautilus::UInt64>(entrySize),
                           Nautilus::Value<Nautilus::UInt64>(numberOfBins));

    // Calling the reset function across all bins
    auto binsMemRef = Nautilus::FunctionCall("getBinsRefProxy", getBinsRefProxy, opHandler);
    auto binsRef = Nautilus::Interface::Fixed2DArrayRef(binsMemRef, entrySize, numberOfBins);

    for (Nautilus::Value<> bin = 0_u64; bin < numberOfBins; bin = bin + 1) {
        aggregationFunction->reset(binsRef[0][bin]);
    }
}

bool EquiWidthOneDimensionalHistogram::storeLocalOperatorState(const uint64_t handlerIndex,
                                                               const Runtime::Execution::Operators::Operator* op,
                                                               Runtime::Execution::ExecutionContext& ctx) {
    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    auto binsMemRef = Nautilus::FunctionCall("getBinsRefProxy", getBinsRefProxy, opHandler);
    auto binsRef = Nautilus::Interface::Fixed2DArrayRef(binsMemRef, entrySize, numberOfBins);

    ctx.setLocalOperatorState(op, std::make_unique<LocalBinsOperatorState>(binsRef));
    return true;
}

EquiWidthOneDimensionalHistogram::EquiWidthOneDimensionalHistogram(Parsing::SynopsisAggregationConfig &aggregationConfig, const uint64_t entrySize,
                                 const int64_t minValue, const int64_t maxValue, const uint64_t numberOfBins,
                                 std::string  lowerBinBoundString, const std::string& upperBinBoundString)
        : AbstractSynopsis(aggregationConfig), minValue(minValue), numberOfBins(numberOfBins),
        binWidth((maxValue - minValue) / numberOfBins), entrySize(entrySize), lowerBinBoundString(std::move(lowerBinBoundString)),
        upperBinBoundString(upperBinBoundString){
}
} // namespace NES::ASP