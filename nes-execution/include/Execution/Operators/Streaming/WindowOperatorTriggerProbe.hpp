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
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/// Is the general probe operator for window operators. It is responsible for the emitting of slices and windows to the second phase (probe).
class WindowOperatorTriggerProbe : public ExecutableOperator {
public:
    explicit WindowOperatorTriggerProbe(uint64_t operatorHandlerIndex);

    /// Passes emits slices that are ready to the second join phase (probe) for further processing
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    /// Emits/Flushes all slices and windows, as the query will be terminated
    void terminate(ExecutionContext& executionCtx) const override;

protected:
    const uint64_t operatorHandlerIndex;
};

}
