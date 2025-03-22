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
#include <Functions/PhysicalFunction.hpp>
#include <Streaming/Aggregation/WindowAggregation.hpp>
#include <Streaming/WindowBuildPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{

class AggregationBuildPhysicalOperator final : public WindowAggregation,  public WindowBuildPhysicalOperator
{
public:
    AggregationBuildPhysicalOperator(
        const uint64_t operatorHandlerIndex,
        std::shared_ptr<TimeFunction> timeFunction,
        std::vector<Functions::PhysicalFunction> keyFunctions,
        std::shared_ptr<WindowAggregation> windowAggregationOperator);
    void execute(ExecutionContext& ctx, Record& record) const override;

    std::optional<PhysicalOperator> getChild() const override { return child; }
    void setChild(struct PhysicalOperator child) override { this->child = child; }

private:
    std::optional<PhysicalOperator> child;
    const std::vector<Functions::PhysicalFunction> keyFunctions;
    static constexpr bool PIPELINE_BREAKER = false;
};

}
