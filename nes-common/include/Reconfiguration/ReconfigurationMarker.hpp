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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKER_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKER_HPP_

#include <Reconfiguration/ReconfigurationMarkerEvent.hpp>
#include <optional>
#include <string>
#include <unordered_map>

namespace NES {

class ReconfigurationMarker;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;
/**
 * @brief This class contains the information about different state
 */
class ReconfigurationMarker {
  public:
    static ReconfigurationMarkerPtr create();
    ReconfigurationMarker() = default;
    std::optional<ReconfigurationMarkerEventPtr> getReconfigurationEvent(const std::string& key);
    void addReconfigurationEvent(const std::string& key, const ReconfigurationMarkerEventPtr& reconfigurationEvent);

  private:
    std::unordered_map<std::string, ReconfigurationMarkerEventPtr> reconfigurationEvents;
};

}// namespace NES

#endif// NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKER_HPP_
