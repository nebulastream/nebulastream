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

#ifndef NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_STATCOLLECTOR_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_STATCOLLECTOR_HPP

#include <iostream>
#include <string>
#include <vector>
#include <memory>

namespace NES::Experimental::Statistics {

  class StatCollectorConfig;

  class StatCollector {
    public:

      using StatCollectorPtr = std::unique_ptr<StatCollector>;

      virtual ~StatCollector() = default;

      [[nodiscard]] const std::string& getPhysicalSourceName() const;
      [[nodiscard]] const std::string& getField() const;
      [[nodiscard]] time_t getDuration() const;
      [[nodiscard]] time_t getFrequency() const;
      StatCollector(const StatCollectorConfig config);
      virtual void update(uint32_t key) = 0;
      virtual bool equal(const StatCollectorPtr& rightStatCollector, bool statCollection) = 0;
      virtual StatCollectorPtr merge(StatCollectorPtr rightStatCollector, bool statCollection) = 0;

    private:
      const std::string physicalSourceName;
      const std::string field;
      const time_t duration;
      const time_t frequency;
  };

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_STATCOLLECTOR_HPP
