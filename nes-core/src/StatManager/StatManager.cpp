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

#include "StatManager/StatManager.hpp"
#include "StatManager/StatCollectors/StatCollector.hpp"

namespace NES {

  std::vector<NES::StatCollector>& StatManager::getStatCollectors() {
    return statCollectors;
  }

  void StatManager::createStat (const std::string& physicalSourceName,
                   const std::string& field/*,
                   const std::string& statName*/) {

    bool found = 0;

    // check if the stat is already being tracked passively
    for (const auto& collector : statCollectors) {
      if (collector.getPhysicalSourceName() == physicalSourceName && collector.getField() == field) {
        found = 1;
        break;
      }
    }

    // if the stat is not yet tracked, then create it
    if(!found) {

    }

  }

//  double queryStat (const std::string& physicalSourceName,
//                    const std::string& field,
//                    const std::string& statName) {
//
//    return 0.0;
//  }
//
//  void deleteStat (const std::string& physicalSourceName,
//                   const std::string& field,
//                   const std::string& statName) {
//
//  }

} // NES

