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
#include <vector>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <PhysicalOperator.hpp>


namespace NES
{

/// Is the general probe operator for window operators. It is responsible for garbage collecting slices and windows.
/// It is part of the second phase (probe) that processes the build up state of the first phase (build).
class WindowProbePhysicalOperator : public PhysicalOperatorConcept
{
public:
    explicit WindowProbePhysicalOperator(
        OperatorHandlerId operatorHandlerIndex, std::string windowStartFieldName, std::string windowEndFieldName);

    /// The setup method is called for each pipeline during the query initialization procedure. Meaning that if
    /// multiple pipelines with the same operator (e.g. JoinBuild) have access to the same operator handler, this will lead to race conditions.
    /// Therefore, any setup to the operator handler should ONLY happen in the WindowProbePhysicalOperator.
    void setup(ExecutionContext& executionCtx) const override;

    /// Checks the current watermark and then deletes all slices and windows that are not valid anymore
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    std::optional<PhysicalOperator> getChild() const override { return child; }
    void setChild(struct PhysicalOperator child) override { this->child = child; }

protected:
    std::optional<PhysicalOperator> child;
    OperatorHandlerId operatorHandlerIndex;
    std::string windowStartFieldName;
    std::string windowEndFieldName;
};

}
