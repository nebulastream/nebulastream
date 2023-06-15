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
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

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


void EquiWidthOneDimensionalHistogram::addToSynopsis(uint64_t, Runtime::Execution::ExecutionContext&,
                                    Nautilus::Record record,
                                    Runtime::Execution::Operators::OperatorState *pState) {

    // Retrieving the reference to the bins
    NES_ASSERT2_FMT(pState != nullptr, "Local state was null, but we expected it not to be null!");
    auto localState = dynamic_cast<LocalBinsOperatorState*>(pState);
    auto& bins = localState->bins;

    auto binDimensionPos = ((readKeyExpression->execute(record) - minValue) / binWidth);
    aggregationFunction->lift(bins[(uint64_t)0][binDimensionPos], record);
}

std::vector<Runtime::TupleBuffer> EquiWidthOneDimensionalHistogram::getApproximate(uint64_t handlerIndex,
                                                                                   Runtime::Execution::ExecutionContext &ctx,
                                                                                   std::vector<Nautilus::Value<>>& keys,
                                                                                   Runtime::BufferManagerPtr bufferManager) {
    using namespace Runtime::Execution;

    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    auto binsMemRef = Nautilus::FunctionCall("getBinsRefProxy", getBinsRefProxy, opHandler);
    auto bins = Nautilus::Interface::Fixed2DArrayRef(binsMemRef, entrySize, numberOfBins);

    auto memoryProviderOutput = MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(),
                                                                                     outputSchema);
    auto buffer = bufferManager->getBufferBlocking();
    auto maxRecordsPerBuffer = bufferManager->getBufferSize() / outputSchema->getSchemaSizeInBytes();
    std::vector<Runtime::TupleBuffer> retTupleBuffers;
    Nautilus::Value<Nautilus::UInt64> recordIndex((uint64_t) 0);

    for (auto& key : keys) {
        auto binDimensionPos = ((key - minValue) / binWidth);
        Nautilus::Record record;
        aggregationFunction->lower(bins[(uint64_t) 0][binDimensionPos], record);

        // Writing the key, lower and upper value of the current bin
        auto lowerBinBound = binDimensionPos * binWidth;
        auto upperBinBound = (binDimensionPos + 1) * binWidth;
        record.write(lowerBinBoundString, lowerBinBound);
        record.write(upperBinBoundString, upperBinBound);
        record.write(fieldNameKey, key);

        // Writing the values to the buffer
        auto recordBuffer = RecordBuffer(Nautilus::Value<Nautilus::MemRef>((int8_t*) std::addressof(buffer)));
        auto bufferAddress = recordBuffer.getBuffer();
        memoryProviderOutput->write(recordIndex, bufferAddress, record);
        recordIndex = recordIndex + 1;
        recordBuffer.setNumRecords(recordIndex);

        if (recordIndex >= maxRecordsPerBuffer) {
            retTupleBuffers.emplace_back(buffer);
            buffer = bufferManager->getBufferBlocking();
            recordIndex = (uint64_t) 0;
        }
    }

    if (recordIndex > 0) {
        retTupleBuffers.emplace_back(buffer);
    }

    return retTupleBuffers;
}

void EquiWidthOneDimensionalHistogram::setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) {
    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    Nautilus::FunctionCall("setupOpHandlerProxy", setupOpHandlerProxy, opHandler,
                           Nautilus::Value<Nautilus::UInt64>(entrySize),
                           Nautilus::Value<Nautilus::UInt64>(numberOfBins));

    // Calling the reset function across all bins
    auto binsMemRef = Nautilus::FunctionCall("getBinsRefProxy", getBinsRefProxy, opHandler);
    auto binsRef = Nautilus::Interface::Fixed2DArrayRef(binsMemRef, entrySize, numberOfBins);

    for (Nautilus::Value<> bin = (uint64_t) 0; bin < numberOfBins; bin = bin + 1) {
        aggregationFunction->reset(binsRef[(uint64_t)0][bin]);
    }
}

bool EquiWidthOneDimensionalHistogram::storeLocalOperatorState(uint64_t handlerIndex, const Runtime::Execution::Operators::Operator* op,
                                              Runtime::Execution::ExecutionContext& ctx,
                                              Runtime::Execution::RecordBuffer) {
    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    auto binsMemRef = Nautilus::FunctionCall("getBinsRefProxy", getBinsRefProxy, opHandler);
    auto binsRef = Nautilus::Interface::Fixed2DArrayRef(binsMemRef, entrySize, numberOfBins);

    ctx.setLocalOperatorState(op, std::make_unique<LocalBinsOperatorState>(binsRef));
    return true;
}

EquiWidthOneDimensionalHistogram::EquiWidthOneDimensionalHistogram(Parsing::SynopsisAggregationConfig &aggregationConfig, const uint64_t entrySize,
                                 const int64_t minValue, const int64_t maxValue, const uint64_t numberOfBins,
                                 const std::string& lowerBinBoundString, const std::string& upperBinBoundString)
        : AbstractSynopsis(aggregationConfig), minValue(minValue), maxValue(maxValue), numberOfBins(numberOfBins),
        binWidth((maxValue - minValue) / numberOfBins), entrySize(entrySize), lowerBinBoundString(lowerBinBoundString),
        upperBinBoundString(upperBinBoundString){
}
} // namespace NES::ASP