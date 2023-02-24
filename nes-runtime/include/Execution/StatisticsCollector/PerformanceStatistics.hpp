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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_PERFORMANCESTATISTICS_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_PERFORMANCESTATISTICS_HPP_

#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>
#include <Execution/StatisticsCollector/Profiler.hpp>

namespace NES::Runtime::Execution {
/**
 * @brief Implements a statistic that collects the runtime of a pipeline.
 */
class PerformanceStatistics : public Statistic {
  public:
    PerformanceStatistics();
    void collect() override;

    /**
    * @brief Get the pipelineId of the pipeline for which the runtime is collected.
    * @return pipelineId
    */
    std::string getType() const override;

    void startProfiling();

  private:
    Profiler profiler;
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_PERFORMANCESTATISTICS_HPP_