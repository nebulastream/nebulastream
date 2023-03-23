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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_NORMALIZER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_NORMALIZER_HPP_

#include <cstdint>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetectorWrapper.hpp>
#include <vector>

namespace NES::Runtime::Execution {
/**
 * @brief Normalizes the values for change detectors.
 */
class Normalizer {
  public:
    /**
    * @brief Initialize class that normalizes the values of the statistics.
    * @param windowSize size of the window used to find the maximum for normalization.
    * @param changeDetectorWrapper change detector to which normalized values are added.
    */
    Normalizer(uint64_t windowSize, std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper);

    /**
    * @brief Normalize the value.
    * @param value to be normalized.
    */
    void normalizeValue(uint64_t value);

  private:
    /**
    * @brief Normalize the values in the window and add them to the change detector.
    */
    void addNormalizedValuesToChangeDetection();

    size_t windowSize;
    std::vector<uint64_t> window;
    std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper;
    uint64_t max;
};
}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_NORMALIZER_HPP_