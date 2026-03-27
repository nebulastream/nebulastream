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
#include <optional>
#include <string>
#include <vector>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <CompilationContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// @brief Physical operator that runs IREE model inference on each record.
class InferModelPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    InferModelPhysicalOperator(
        OperatorHandlerId handlerId,
        std::vector<std::string> inputFieldNames,
        std::vector<std::string> outputFieldNames,
        bool varsizedInput,
        bool varsizedOutput);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    OperatorHandlerId handlerId;
    std::vector<std::string> inputFieldNames;
    std::vector<std::string> outputFieldNames;
    bool varsizedInput;
    bool varsizedOutput;
    std::optional<PhysicalOperator> child;
};

}
