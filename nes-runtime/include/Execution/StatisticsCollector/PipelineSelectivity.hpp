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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_PIPELINESELECTIVITY_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_PIPELINESELECTIVITY_HPP_

#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>
#include <any>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetectorWrapper.hpp>

namespace NES::Runtime::Execution {
/**
 * @brief Implements a statistic that collects the selectivity of a pipeline.
 */
class PipelineSelectivity : public Statistic {
  public:
    /**
     * @brief Initialize statistic to collect the selectivity of a pipeline.
     * @param changeDetector that monitors the selectivity for changes.
     * @param nautilusExecutablePipelineStage
     */
    PipelineSelectivity(std::unique_ptr<ChangeDetector> changeDetector,
                        std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage);
    bool collect() override;
    std::any getStatisticValue() override;

  private:
    std::unique_ptr<ChangeDetector> changeDetector;
    const std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage;
    double selectivity {};
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_PIPELINESELECTIVITY_HPP_