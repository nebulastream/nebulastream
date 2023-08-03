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

#include "StatManager/StatCollectors/StatCollector.hpp"
//#include "StatManager/StatCollectors/Synopses/Sketches/CountMin/ddsketch.hpp"
//#include "StatManager/StatCollectors/Synopses/Sketches/CountMin/hyperLogLog.hpp"
//#include "StatManager/StatCollectors/Synopses/Sketches/CountMin/reservoirSample.hpp"

namespace NES {

  const std::string& StatCollector::getPhysicalSourceName() const {
    return physicalSourceName;
  }

  const std::string& StatCollector::getField() const {
    return field;
  }

  time_t StatCollector::getDuration() const {
    return duration;
  }

  time_t StatCollector::getFrequency() const {
    return frequency;
  }

  StatCollector::StatCollector(const Configurations::StatManagerConfig config)
  : physicalSourceName(static_cast<std::string>(config.physicalSourceName.getValue())),
  field(static_cast<std::string>(config.field.getValue())),
  duration(static_cast<time_t>(config.duration.getValue())),
  frequency(static_cast<time_t>(config.frequency.getValue())) {

  }


} // NES