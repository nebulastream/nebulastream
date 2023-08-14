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

#ifndef NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP

#include <string>
#include <vector>
#include <memory>
#include <math.h>
#include <StatManager/StatCollectors/StatCollector.hpp>

namespace NES::Experimental::Statistics {

  class StatCollectorConfig;

  class StatManager {
  private:
    std::vector<std::unique_ptr<StatCollector>> statCollectors {};

  public:
    ~StatManager() = default;

    std::vector<std::unique_ptr<StatCollector>>& getStatCollectors();

    void createStat(const StatCollectorConfig& config);

    double queryStat(const StatCollectorConfig& config,
                     const uint32_t key);

    void deleteStat(const StatCollectorConfig& config);
  };

}

#endif //NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP
