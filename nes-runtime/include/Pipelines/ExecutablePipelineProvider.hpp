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
#include <Runtime/Execution/OperatorHandler.hpp>
#include <nautilus/options.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES
{
class ExecutablePipelineStage;

/// The executable pipeline provider creates an executable pipeline stage out of a pipeline of physical operators.
/// We differentiate between different implementations that can use different execution strategies, e.g. compilation or interpretation.
class ExecutablePipelineProvider
{
public:
    /// Creates an executable pipeline for the pipeline of physical operators.
    /// Options for the nautilus compilation backend: https://github.com/nebulastream/nautilus/blob/main/docs/options.md
    virtual std::unique_ptr<ExecutablePipelineStage> create(
        std::unique_ptr<Pipeline> pipeline,
        std::vector<std::unique_ptr<OperatorHandler>> operatorHandlers,
        nautilus::engine::Options& options) = 0;
    virtual ~ExecutablePipelineProvider() = default;
};

}
