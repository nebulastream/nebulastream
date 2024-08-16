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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedWindowEmitAction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>

namespace NES::Runtime::Execution::Operators {

void* getKeyedSliceState(void* gs);

void deleteSlice(void* gs) {
    auto globalSlice = static_cast<KeyedSlice*>(gs);
    delete globalSlice;
}

KeyedWindowEmitAction::KeyedWindowEmitAction(
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions,
    const std::string startTsFieldName,
    const std::string endTsFieldName,
    const uint64_t keySize,
    const uint64_t valueSize,
    const std::vector<std::string> resultKeyFields,
    const std::vector<PhysicalTypePtr> keyDataTypes,
    const OriginId resultOriginId)
    : aggregationFunctions(aggregationFunctions), startTsFieldName(startTsFieldName), endTsFieldName(endTsFieldName),
      keySize(keySize), valueSize(valueSize), resultKeyFields(resultKeyFields), keyDataTypes(keyDataTypes),
      resultOriginId(resultOriginId) {}

void KeyedWindowEmitAction::emitSlice(ExecutionContext& ctx,
                                      ExecuteOperatorPtr& child,
                                      ExecDataUI64& windowStart,
                                      ExecDataUI64& windowEnd,
                                      ExecDataUI64& sequenceNumber,
                                      ExecDataUI64& chunkNumber,
                                      ExecDataBool& lastChunk,
                                      ObjRefVal<void>& globalSlice) const {
    ctx.setWatermarkTs(castAndLoadValue<uint64_t>(windowStart));
    ctx.setOrigin(resultOriginId.getRawValue());
    ctx.setSequenceNumber(castAndLoadValue<uint64_t>(sequenceNumber));
    ctx.setChunkNumber(castAndLoadValue<uint64_t>(chunkNumber));
    ctx.setLastChunk(castAndLoadValue<bool>(lastChunk));

    ((void) windowEnd);
    ((void) globalSlice);
    ((void) child);
    ((void) ctx);
    ((void) windowStart);
    ((void) sequenceNumber);
    ((void) chunkNumber);
    ((void) lastChunk);

    NES_INFO("Emitting tuples...");

    auto globalSliceState = nautilus::invoke(getKeyedSliceState, globalSlice);
    auto globalHashTable = Interface::ChainedHashMapRef(globalSliceState, keyDataTypes, keySize, valueSize);
    // create the final window content and emit it to the downstream operator
    for (const auto& globalEntry : globalHashTable) {
        Record resultWindow;
        // write window start and end to result record
        resultWindow.write(startTsFieldName, windowStart);
        resultWindow.write(endTsFieldName, windowEnd);
        // load keys and write them to result record
        auto sliceKeys = globalEntry.getKeyPtr();
        for (nautilus::static_val<size_t> i = 0; i < resultKeyFields.size(); ++i) {
            const auto value = Nautilus::readExecDataTypeFromMemRef(sliceKeys, keyDataTypes[i]);
            resultWindow.write(resultKeyFields[i], value);
            sliceKeys = sliceKeys + nautilus::val<uint64_t>(keyDataTypes[i]->size());
        }
        // load values and write them to result record
        auto sliceValue = globalEntry.getValuePtr();
        for (const auto& aggregationFunction : nautilus::static_iterable(aggregationFunctions)) {
            aggregationFunction->lower(sliceValue, resultWindow);
            sliceValue = sliceValue + nautilus::val<uint64_t>(aggregationFunction->getSize());
        }
        // If we get rid of the .toString(), we receive another error
        // NES_INFO("Emitting window: {}", resultWindow.toString());
//        std::cout << "Emitting window: " << std::endl;
        child->execute(ctx, resultWindow);
    }
    nautilus::invoke(deleteSlice, globalSlice);
}
}// namespace NES::Runtime::Execution::Operators
