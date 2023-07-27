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
#include "StatManager/StatCollectors/StatCollector.hpp"

namespace NES {

  class StatManager {
  private:
    std::vector<NES::StatCollector*> statCollectors;

  public:
    virtual ~StatManager() = default;

    std::vector<NES::StatCollector*>& getStatCollectors();

    void createStat(const std::string& physicalStreamName,
                    const std::string& field,
                    const std::string& statName,
                    const time_t duration,
                    const time_t frequency);

//    double queryStat(const std::string& physicalStreamName,
//                     const std::string& field,
//                     const std::string& statName);

//    void deleteStat(const std::string& physicalStreamName);

//    void deleteStat(const std::string& physicalStreamName,
//                    const std::string& field);

//    void deleteStat(const std::string& physicalStreamName,
//                    const std::string& field,
//                    const std::string& statName);
  };

}

#endif //NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP
