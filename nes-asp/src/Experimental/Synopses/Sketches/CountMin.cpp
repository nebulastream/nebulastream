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
#include <Experimental/Synopses/Sketches/CountMin.hpp>
#include <Experimental/Synopses/Sketches/CountMinOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::ASP {

void* getSketchArrayProxy(void* opHandlerPtr) {
    NES_ASSERT2_FMT(opHandlerPtr != nullptr, "opHandlerPtr can not be null");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);
    return opHandler->getSketchRef();
}

void* getH3SeedsProxy(void* opHandlerPtr) {
    NES_ASSERT2_FMT(opHandlerPtr != nullptr, "opHandlerPtr can not be null");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);
    return opHandler->getH3SeedsRef();
}

void setupOpHandlerProxy(void* opHandlerPtr, uint64_t entrySize, uint64_t numberOfRows, uint64_t numberOfCols) {
    NES_ASSERT2_FMT(opHandlerPtr != nullptr, "opHandlerPtr can not be null");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);
    opHandler->setup(entrySize, numberOfRows, numberOfCols);
}


void CountMin::addToSynopsis(const uint64_t, Runtime::Execution::ExecutionContext&, Nautilus::Record record,
                             const Runtime::Execution::Operators::OperatorState *pState) {

    NES_ASSERT2_FMT(pState != nullptr, "Local state was null, but we expected it not to be null!");
    auto localState = dynamic_cast<const LocalCountMinState*>(pState);
    auto& sketchArray = localState->sketchArray;
    auto& h3SeedsMemRef = localState->h3SeedsMemRef;

    auto key = readKeyExpression->execute(record);
    for (Nautilus::Value<> row = 0; row < numberOfRows; row = row + 1) {
        auto h3SeedsCurRow = (h3SeedsMemRef + row * numberOfCols * entrySize).as<Nautilus::MemRef>();
        auto hash = h3HashFunction->calculateWithState(key, h3SeedsCurRow);
        auto colPos = hash % numberOfCols;
        NES_DEBUG2("Colpos {} for key {}", colPos.getValue().toString(), key.getValue().toString());
        aggregationFunction->lift(sketchArray[row][colPos], record);
    }
}

void CountMin::getApproximateRecord(const uint64_t handlerIndex, Runtime::Execution::ExecutionContext& ctx,
                                    const Nautilus::Value<>& key, Nautilus::Record& outputRecord) {
    using namespace Runtime::Execution;

    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    auto h3SeedsMemRef = Nautilus::FunctionCall("getH3SeedsProxy", getH3SeedsProxy, opHandler);
    auto sketchArrayMemRef = Nautilus::FunctionCall("getSketchArrayProxy", getSketchArrayProxy, opHandler);
    auto sketchArray = Nautilus::Interface::Fixed2DArrayRef(sketchArrayMemRef, entrySize, numberOfCols);

    // Calculating the position
    auto hashFirstRow = h3HashFunction->calculateWithState(key, h3SeedsMemRef);
    auto colPosFirstRow = hashFirstRow % numberOfCols;

    // Getting the actual value out over all rows
    for (Nautilus::Value<> row = 1; row < numberOfRows; row = row + 1) {
        auto h3SeedsCurRow = (h3SeedsMemRef + row * numberOfCols * entrySize).as<Nautilus::MemRef>();
        auto hash = h3HashFunction->calculateWithState(key, h3SeedsCurRow);
        auto colPos = hash % numberOfCols;
        combineRowsApproximate->combine(sketchArray[(uint64_t) 0][colPosFirstRow], sketchArray[row][colPos]);
    }

    // Writing the key and the approximation to the record
    outputRecord.write(fieldNameKey, key);
    combineRowsApproximate->lower(sketchArray[(uint64_t) 0][colPosFirstRow], outputRecord);
}

void CountMin::setup(const uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) {
    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    Nautilus::FunctionCall("setupOpHandlerProxy", setupOpHandlerProxy, opHandler,
                           Nautilus::Value<Nautilus::UInt64>(entrySize),
                           Nautilus::Value<Nautilus::UInt64>(numberOfRows),
                           Nautilus::Value<Nautilus::UInt64>(numberOfCols));

    auto sketchArrayMemRef = Nautilus::FunctionCall("getSketchArrayProxy", getSketchArrayProxy, opHandler);
    auto sketchArray = Nautilus::Interface::Fixed2DArrayRef(sketchArrayMemRef, entrySize, numberOfCols);
    for (Nautilus::Value<> row = (uint64_t) 0; row < numberOfRows; row = row + 1) {
        for (Nautilus::Value<> col = (uint64_t) 0; col < numberOfCols; col = col + 1) {
            aggregationFunction->reset(sketchArray[row][col]);
        }
    }
}

bool CountMin::storeLocalOperatorState(const uint64_t handlerIndex, const Runtime::Execution::Operators::Operator *op,
                                       Runtime::Execution::ExecutionContext &ctx) {

    auto opHandler = ctx.getGlobalOperatorHandler(handlerIndex);
    auto h3SeedsMemRef = Nautilus::FunctionCall("getH3SeedsProxy", getH3SeedsProxy, opHandler);
    auto sketchArrayMemRef = Nautilus::FunctionCall("getSketchArrayProxy", getSketchArrayProxy, opHandler);
    auto sketchArray = Nautilus::Interface::Fixed2DArrayRef(sketchArrayMemRef, entrySize, numberOfCols);

    ctx.setLocalOperatorState(op, std::make_unique<LocalCountMinState>(sketchArray, h3SeedsMemRef));
    return true;
}

CountMin::CountMin(Parsing::SynopsisAggregationConfig &aggregationConfig, const uint64_t numberOfRows,
                   const uint64_t numberOfCols, const uint64_t entrySize, const uint64_t numberOfKeyBits)
                   : AbstractSynopsis(aggregationConfig), numberOfRows(numberOfRows), numberOfCols(numberOfCols),
                   entrySize(entrySize), h3HashFunction(std::make_unique<Nautilus::Interface::H3Hash>(numberOfKeyBits)) {

    auto inputType = aggregationConfig.getInputType();
    auto finalType = aggregationConfig.getFinalType();
    auto readFieldExpression = aggregationConfig.getReadFieldAggregationExpression();
    auto resultFieldIdentifier = aggregationConfig.fieldNameApproximate;

    /**
     * @brief For each aggregation type, we might have different keys mapped to the same column for each row. Therefore, we
     * choose the combining aggregation function, such that we reduce the error.
     */
    switch (aggregationType) {
        case Parsing::Aggregation_Type::MAX:
        case Parsing::Aggregation_Type::SUM:
        case Parsing::Aggregation_Type::COUNT:
        {
            combineRowsApproximate =  std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(inputType, finalType,
                                                                                             readFieldExpression,
                                                                                             resultFieldIdentifier);
            break;
        }

        case Parsing::Aggregation_Type::MIN: {
            combineRowsApproximate = std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(
                    inputType, finalType,
                    readFieldExpression,
                    resultFieldIdentifier);
            break;
        }
        case Parsing::Aggregation_Type::AVERAGE: {
            combineRowsApproximate = std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(
                    inputType, finalType,
                    readFieldExpression,
                    resultFieldIdentifier);
            break;
        }
        case Parsing::Aggregation_Type::NONE: NES_NOT_IMPLEMENTED();
    }
}
} // namespace NES::ASP