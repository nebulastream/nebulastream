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
#include <Execution/Operators/Statistics/CountMinBuildOperator.hpp>
#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>
#include <Util/StatisticCollectorType.hpp>

namespace NES::Experimental::Statistics {

CountMinBuildOperator::CountMinBuildOperator(uint64_t operatorHandlerIndex,
                                             uint64_t width,
                                             uint64_t depth,
                                             const std::string& onField,
                                             uint64_t keySizeInBits,
                                             Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
                                             std::vector<uint64_t> h3Seeds)
    : operatorHandlerIndex(operatorHandlerIndex), width(width), depth(depth), onField(onField), keySizeInBits(keySizeInBits),
      timeFunction(std::move(timeFunction)), h3Seeds(h3Seeds) {}

bool countMinExistsProxy(void* opHandlerPtr, uint64_t ts) {
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);
    return opHandler->countMinExists(ts);
}

void createAndInsertCountMinProxy(void* opHandlerPtr,
                         uint64_t depth,
                         uint64_t width,
                         const Nautilus::TextValue* nLogicalSourceName,
                         const Nautilus::TextValue* nPhysicalSourceName,
                         WorkerId workerId,
                         const Nautilus::TextValue* nFieldName,
                         uint64_t ts) {
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);

    auto logicalSourceName = std::string(nLogicalSourceName->c_str());
    auto physicalSourceName = std::string(nPhysicalSourceName->c_str());
    auto fieldName = std::string(nFieldName->c_str());

    auto statisticCollectorIdentifier = std::make_shared<StatisticCollectorIdentifier>(logicalSourceName,
                                                                                       physicalSourceName,
                                                                                       workerId,
                                                                                       fieldName,
                                                                                       StatisticCollectorType::COUNT_MIN);
    opHandler->createAndInsertCountMin(depth, width, statisticCollectorIdentifier, ts);
}

void incrementCounterProxy(void* opHandlerPtr, uint64_t rowId, uint64_t colId, uint64_t ts) {
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);
    opHandler->incrementCounter(rowId, colId, ts);
}

void CountMinBuildOperator::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    Nautilus::Value<Nautilus::UInt64> timestampVal = timeFunction->getTs(ctx, record);
    Nautilus::Value<Nautilus::UInt64> nWidth = width;
    Nautilus::Value<Nautilus::UInt64> nDepth = depth;
    auto logicalSourceName = record.read(LOGICAL_SOURCE_NAME).as<Nautilus::Text>();
    auto physicalSourceName = record.read(PHYSICAL_SOURCE_NAME).as<Nautilus::Text>();
    auto workerId = record.read(WORKER_ID).as<Nautilus::UInt64>();
    auto fieldName = record.read(FIELD_NAME).as<Nautilus::Text>();

    for (Nautilus::Value<Nautilus::UInt64> rowId = 0_u64; rowId < depth; rowId = rowId + 1) {

        // check if CountMin sketch exists and if not, then create one
        if (!FunctionCall("countMinExistsProxy", countMinExistsProxy, globalOperatorHandler, timestampVal)) {
            FunctionCall("createAndInsertCountMinProxy",
                         createAndInsertCountMinProxy,
                         globalOperatorHandler,
                         nDepth,
                         nWidth,
                         logicalSourceName->getReference(),
                         physicalSourceName->getReference(),
                         workerId,
                         fieldName->getReference(),
                         timestampVal);
        }

        Nautilus::Value<Nautilus::MemRef> h3SeedsMemRef((int8_t*) h3Seeds.data());
        h3SeedsMemRef = h3SeedsMemRef + (sizeof(uint64_t) * width * rowId).as<Nautilus::MemRef>();
        Nautilus::Value<Nautilus::UInt64> columnId = 0_u64;
        auto key = record.read(onField);
        Nautilus::Interface::H3Hash(keySizeInBits).calculateWithState(columnId, key, h3SeedsMemRef);
        columnId = columnId % width;
        FunctionCall("incrementCounterProxy", incrementCounterProxy, globalOperatorHandler, rowId, columnId, timestampVal);
    }
}

}// namespace NES::Experimental::Statistics