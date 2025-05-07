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

namespace NES::Runtime::Execution::Operators
{

class IREEInferenceOperator : public ExecutableOperator
{
public:
    IREEInferenceOperator(
        const uint32_t inferModelHandlerIndex,
        const std::vector<std::string>& inputFieldNames,
        const std::vector<std::string>& outputFieldNames)
        : inferModelHandlerIndex(inferModelHandlerIndex), inputFieldNames(inputFieldNames), outputFieldNames(outputFieldNames) { };

    void execute(ExecutionContext& ctx, Record& record) const override;

    void setup(ExecutionContext& executionCtx) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    bool isVarSizedInput = false;
    bool isVarSizedOutput = false;
    size_t outputSize = 0;

private:
    const uint32_t inferModelHandlerIndex;
    const std::vector<std::string> inputFieldNames;
    const std::vector<std::string> outputFieldNames;
};

}
