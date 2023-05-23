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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDETECTOR_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDETECTOR_HPP_

#include <mutex>

namespace NES::Runtime::Execution {
/**
 * @brief Wrapper class for change detectors.
 */
class ChangeDetector{

  public:

    /**
     * @brief Insert value into change detector.
     * @return true if change detected, false otherwise
     */
    virtual bool insertValue(double& value) = 0;

    /**
     * @brief Get estimated mean.
     * @return estimated mean
     */
    virtual double getMeanEstimation() = 0;

    /**
     * @brief Reset change detector, e.g., after Normalizer found new maximum value for normalization.
     */
    virtual void reset() = 0;

    virtual ~ChangeDetector() = default;

    std::mutex mutex;

};
} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDETECTOR_HPP_