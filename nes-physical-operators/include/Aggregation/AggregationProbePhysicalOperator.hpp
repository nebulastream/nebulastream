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
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <HashMapOptions.hpp>
#include <WindowProbePhysicalOperator.hpp>

namespace NES
{

class AggregationProbePhysicalOperator final : public WindowProbePhysicalOperator
{
public:
    AggregationProbePhysicalOperator(
        HashMapOptions hashMapOptions,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
        OperatorHandlerId operatorHandlerId,
        WindowMetaData windowMetaData);
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    HashMapOptions hashMapOptions;
};

}
