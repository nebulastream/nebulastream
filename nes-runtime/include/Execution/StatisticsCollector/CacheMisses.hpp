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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_CACHEMISSES_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_CACHEMISSES_HPP_

#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/Normalizer.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>
#include <Execution/StatisticsCollector/Profiler.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetectorWrapper.hpp>

namespace NES::Runtime::Execution {
/**
 * @brief Implements a statistic that collects the cache misses of an operator.
 */
class CacheMisses : public Statistic {
  public:
    /**
    * @brief Initialize to collect the cache misses of an operator.
    * @param profiler instance of profiler that measures the branch misses.
    */
    CacheMisses(std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper,
                const std::shared_ptr<Profiler>& profiler,
                uint64_t normalizationWindowSize);
    bool collect() override;
    std::any getStatisticValue() override;

  private:
    std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper;
    std::shared_ptr<Profiler> profiler;
    Normalizer normalizer;
    uint64_t eventId;
    uint64_t cacheMisses{};
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_CACHEMISSES_HPP_