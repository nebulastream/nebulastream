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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP
#define NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP

#include <math.h>
#include <ctime>
#include <string>

namespace NES::Experimental::Statistics {

  class StatCollectorConfig {

    public:
      const std::string &getPhysicalSourceName() const {
        return physicalSourceName;
      }

      const std::string &getField() const {
        return field;
      }

      const std::string &getStatMethodName() const {
        return statMethodName;
      }

      time_t getDuration() const {
        return duration;
      }

      time_t getFrequency() const {
        return frequency;
      }

      double_t getError() const {
        return error;
      }

      double_t getProbability() const {
        return probability;
      }

      uint32_t getDepth() const {
        return depth;
      }

      uint32_t getWidth() const {
        return width;
      }

      // Constructor with parameter initialization
      StatCollectorConfig(const std::string& sourceName = "defaultPhysicalSourceName", const std::string& fieldName = "defaultFieldName",
                                           const std::string& methodName = "defaultStatMethodName", uint32_t dur = 1, uint32_t freq = 100,
                                           double_t err = 0.0001, double_t prob = 0.0001, uint32_t dep = 0, uint32_t wid = 0)
        : physicalSourceName(sourceName), field(fieldName), statMethodName(methodName),
          duration(dur), frequency(freq), error(err), probability(prob),
          depth(dep), width(wid) {
      }

  private:
    const std::string physicalSourceName;
    const std::string field;
    const std::string statMethodName;
    const time_t duration;
    const time_t frequency;
    const double_t error;
    const double_t probability;
    const uint32_t depth;
    const uint32_t width;
  };


} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP
