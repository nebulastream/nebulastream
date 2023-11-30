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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_PIPELINES_EXECUTABLEPIPELINEPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_PIPELINES_EXECUTABLEPIPELINEPROVIDER_HPP_
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Util/PluginRegistry.hpp>
namespace NES::Runtime::Execution {
class ExecutablePipelineStage;

/**
 * @brief The executable pipeline provider creates an executable pipeline stage out of a pipeline of physical operators.
 * We differentiate between different implementations that can use different execution strategies, e.g. compilation or interpretation.
 */
class ExecutablePipelineProvider {
  public:
    /**
     * @brief Creates a executable pipeline for the pipeline of physical operators.
     * @param physicalOperatorPipeline std::shared_ptr<PhysicalOperatorPipeline>
     * @return std::unique_ptr<ExecutablePipelineStage>
     */
    virtual std::unique_ptr<ExecutablePipelineStage> create(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                                            const Nautilus::CompilationOptions&) = 0;
    virtual ~ExecutablePipelineProvider() = default;
};

using ExecutablePipelineProviderRegistry = NES::Util::NamedPluginRegistry<ExecutablePipelineProvider>;

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXECUTION_PIPELINES_EXECUTABLEPIPELINEPROVIDER_HPP_
