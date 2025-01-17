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
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/Execution.hpp>


namespace NES::Runtime::Execution::Operators
{

/// Is the general probe operator for window operators. It is responsible for the garbage collection of slices and windows.
/// It is part of the second phase (probe) that processes the build up state of the first phase (build).
class WindowOperatorProbe : public ExecutableOperator
{
public:
    explicit WindowOperatorProbe(uint64_t operatorHandlerIndex, WindowMetaData windowMetaData);

    /// Initializes the operator handler
    void setup(ExecutionContext& executionCtx) const override;

    /// Checks the current watermark and then deletes all slices and windows that are not valid anymore
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Terminates the operator by deleting all slices and windows
    void terminate(ExecutionContext& executionCtx) const override;

protected:
    const uint64_t operatorHandlerIndex;
    const WindowMetaData windowMetaData;
};

}
