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
#include <functional>
#include <memory>
#include <vector>
#include <Aggregation/WindowAggregation.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

class AggregationBuildPhysicalOperator final : public WindowAggregation, public WindowBuildPhysicalOperator
{
public:
    AggregationBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> timeFunction,
        std::vector<PhysicalFunction> keyFunctions,
        const std::shared_ptr<WindowAggregation>& windowAggregationOperator);
    void execute(ExecutionContext& ctx, Record& record) const override;

    /// Method that gets called, once an aggregation slice gets destroyed.
    [[nodiscard]] std::function<void(const std::vector<std::unique_ptr<Interface::HashMap>>&)> getStateCleanupFunction() const;

private:
    const std::vector<PhysicalFunction> keyFunctions;
};

}
