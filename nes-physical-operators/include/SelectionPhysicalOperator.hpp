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

#include <Functions/PhysicalFunction.hpp>
#include <Abstract/PhysicalOperator.hpp>

namespace NES
{

/// @brief Selection operator that evaluates an boolean function on each record.
class SelectionPhysicalOperator final : public PhysicalOperator
{
public:
    SelectionPhysicalOperator(std::unique_ptr<Functions::PhysicalFunction> function) : function(std::move(function)) {};
    void execute(ExecutionContext& ctx, Record& record) const override;

    std::unique_ptr<Operator> clone() const override;
    std::string toString() const override {return typeid(this).name(); }

private:
    const std::unique_ptr<Functions::PhysicalFunction> function;
    static constexpr bool PIPELINE_BREAKER = false;
};
}
