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
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/Record.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

class AggregationBuildPhysicalOperator final : public WindowBuildPhysicalOperator
{
public:
    AggregationBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> timeFunction,
        std::unique_ptr<SliceStoreRef> sliceStoreRef,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
        ChainedHashMapConfig hashMapConfig,
        std::vector<PhysicalFunction> keyFunctions,
        std::vector<Record::RecordFieldIdentifier> accessedFields);
    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    /// The aggregation function is a shared_ptr, because it is used in the aggregation build and in the getSliceCleanupFunction()
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    ChainedHashMapConfig hashMapConfig;
    /// Extracts the key fields out of an incoming record. Operator logic, not hash map metadata, hence not in the config.
    std::vector<PhysicalFunction> keyFunctions;
    /// The names of the fields accessed by the functions of this aggregation (time function, key functions, aggregation functions).
    /// Only these are deserialized from their LazyValueData form when a record enters the operator.
    /// The remaining fields of a lazily deserialized record stay untouched, saving unnecessary deserializes.
    /// We can remove this member again, once the query compiler is able to recognize and remove unneeded deserializes by itself.
    std::vector<Record::RecordFieldIdentifier> accessedFields;
};

}
