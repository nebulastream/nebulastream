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
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/Streaming/Aggregation/WindowAggregationOperator.hpp>
#include <Execution/Operators/Streaming/WindowOperatorProbe.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>

namespace NES
{

class AggregationProbe final : public WindowAggregationOperator, public WindowOperatorProbe
{
public:
    AggregationProbe(WindowAggregationOperator windowAggregationOperator, uint64_t operatorHandlerIndex, WindowMetaData windowMetaData);
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
};

}
