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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/WindowAggregationOperator.hpp>
#include <Execution/Operators/Streaming/WindowOperatorProbe.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/Execution.hpp>
#include "Execution/Operators/ExecutableOperator.hpp"

namespace NES::Runtime::Execution::Operators
{

class AggregationProbe final : public WindowAggregationOperator, public WindowOperatorProbe
{
public:
    AggregationProbe(
        std::vector<std::unique_ptr<Aggregation::AggregationFunction>> aggregationFunctions,
        std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction,
        std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys,
        std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues,
        uint64_t entriesPerPage,
        uint64_t entrySize,
        uint64_t operatorHandlerIndex,
        WindowMetaData windowMetaData);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
};

}
