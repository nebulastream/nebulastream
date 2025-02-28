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

#include <vector>
#include <cstdint>
#include <PhysicalOperator.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Operators/Windows/WindowOperator.hpp>

namespace NES
{

/// Is the general probe operator for window operators. It is responsible for garbage collecting slices and windows.
/// It is part of the second phase (probe) that processes the build up state of the first phase (build).
class WindowProbePhysicalOperator : public PhysicalOperator
{
public:
    explicit WindowProbePhysicalOperator(uint64_t operatorHandlerIndex, std::string windowStartFieldName, std::string windowEndFieldName);

    /// The setup method is called for each pipeline during the query initialization procedure. Meaning that if
    /// multiple pipelines with the same operator (e.g. JoinBuild) have access to the same operator handler, this will lead to race conditions.
    /// Therefore, any setup to the operator handler should ONLY happen in the WindowProbePhysicalOperator.
    void setup(ExecutionContext& executionCtx) const override;

    /// Checks the current watermark and then deletes all slices and windows that are not valid anymore
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Terminates the operator by deleting all slices and windows
    void terminate(ExecutionContext& executionCtx) const override;

protected:
    uint64_t operatorHandlerIndex;
    std::string windowStartFieldName;
    std::string windowEndFieldName;
};

}
