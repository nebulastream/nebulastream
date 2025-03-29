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

#include <memory>
#include <vector>
#include <Pipelines/ExecutablePipelineProvider.hpp>
#include <options.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES
{

/// @brief Creates an executable pipeline stage that can be executed using interpretation
class InterpretationPipelineProvider : public ExecutablePipelineProvider
{
public:
    std::unique_ptr<ExecutablePipelineStage> create(
        std::shared_ptr<PhysicalOperatorPipeline> pipeline,
        std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers,
        nautilus::engine::Options& options) override;
};
}
