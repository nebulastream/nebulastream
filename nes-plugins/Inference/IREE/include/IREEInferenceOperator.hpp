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

#include <cstddef>
#include <Functions/PhysicalFunction.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

class IREEInferenceOperator : public PhysicalOperatorConcept
{
public:
    IREEInferenceOperator(
        const OperatorHandlerId inferModelHandlerIndex, std::vector<PhysicalFunction> inputs, std::vector<std::string> outputFieldNames)
        : inferModelHandlerIndex(inferModelHandlerIndex), inputs(std::move(inputs)), outputFieldNames(std::move(outputFieldNames)) { }

    void execute(ExecutionContext& ctx, Record& record) const override;
    void setup(ExecutionContext& executionCtx) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<struct PhysicalOperator> getChild() const override { return child; }
    void setChild(PhysicalOperator child) override { this->child = std::move(child); }

    bool isVarSizedInput = false;
    bool isVarSizedOutput = false;
    size_t outputSize = 0;
    size_t inputSize = 0;

private:
    const OperatorHandlerId inferModelHandlerIndex;
    const std::vector<PhysicalFunction> inputs;
    const std::vector<std::string> outputFieldNames;
    std::optional<PhysicalOperator> child;
};

}
