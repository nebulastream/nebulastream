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

#include "IREEAdapter.hpp"
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators
{

class IREEInferenceOperatorHandler : public OperatorHandler
{
public:
    IREEInferenceOperatorHandler(const std::string& model);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    [[nodiscard]] const std::string& getModel() const;
    [[nodiscard]] const std::shared_ptr<IREEAdapter>& getIREEAdapter() const;

private:
    std::string model;
    std::shared_ptr<IREEAdapter> ireeAdapter;

};

}