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

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Model.hpp>

namespace NES
{
class IREEAdapter;
class IREEInferenceOperatorHandler : public OperatorHandler
{
public:
    IREEInferenceOperatorHandler(Nebuli::Inference::Model model);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    [[nodiscard]] const Nebuli::Inference::Model& getModel() const;
    [[nodiscard]] const std::shared_ptr<IREEAdapter>& getIREEAdapter(WorkerThreadId threadId) const;

private:
    Nebuli::Inference::Model model;
    std::vector<std::shared_ptr<IREEAdapter>> threadLocalAdapters;
};

}
