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

#include <memory>
#include <optional>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <WindowProbePhysicalOperator.hpp>

namespace NES
{

/// One output field of the record that a window without tuples emits (SQL semantics of an aggregation without GROUP BY).
struct EmptyWindowField
{
    Record::RecordFieldIdentifier field;
    DataType type;
    /// COUNT is 0 over no tuples, every other aggregation NULL.
    bool isCount;
};

class AggregationProbePhysicalOperator final : public WindowProbePhysicalOperator
{
public:
    AggregationProbePhysicalOperator(
        ChainedHashMapConfig hashMapConfig,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
        OperatorHandlerId operatorHandlerId,
        WindowMetaData windowMetaData,
        std::optional<std::vector<EmptyWindowField>> recordOnEmptyWindow = std::nullopt);

protected:
    void probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    ChainedHashMapConfig hashMapConfig;
    /// Set for aggregations without keys: a window without tuples emits one record (counts 0, everything else NULL)
    /// instead of nothing, see WindowedAggregationLogicalOperator::emitsOnEmptyInput.
    std::optional<std::vector<EmptyWindowField>> recordOnEmptyWindow;
};

}
