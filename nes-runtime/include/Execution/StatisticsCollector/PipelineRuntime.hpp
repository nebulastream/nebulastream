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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_PIPELINERUNTIME_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_PIPELINERUNTIME_HPP_

#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>

namespace NES::Runtime::Execution {
/**
 * @brief Implements a statistic that collects the runtime of a pipeline.
 */
class PipelineRuntime : public Statistic {
  public:
    /**
     * @brief Initialize to collect the runtime of a pipeline with pipelineId.
     * @param nautilusExecutablePipelineStage
     * @param pipelineId id of the pipeline.
     */
    PipelineRuntime(std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage, uint64_t pipelineId);
    void collect() override;

    /**
    * @brief Get the pipelineId of the pipeline for which the runtime is collected.
    * @return pipelineId
    */
    uint64_t getPipelineID() const { return this->pipelineId; }
    std::string getType() const override;

  private:
    const std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage;
    const uint64_t pipelineId;
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_PIPELINERUNTIME_HPP_