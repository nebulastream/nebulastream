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
#include "StatManager/StatCollectors/Synopses/Sketches/CountMin/CountMin.hpp"
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

  StatCollector* StatCollector::createStat(const std::string& physicalSourceName,
      const std::string& field,
      time_t duration,
      time_t interval,
      Yaml::Node configNode) {

    if ((configNode["Type"].As<std::string>()).compare("CountMin") == 0) {

        auto CMSketch = CountMin::initialize( /*physicalSourcePtr,*/ field, duration, interval, configNode);
        StatCollector* statCollectorPtr = CMSketch;
        return statCollectorPtr;

        //    } else if ((configNode["Type"].As<std::string>()).compare("DDSketch") == 0) {
        //
        //      auto myDDSketch = ddsketch::initialize( /*physicalSourcePtr,*/ field, duration, interval, configNode);
        //      statCollector* statCollectorPtr = myDDSketch;
        //      return statCollectorPtr;
        //
        //    } else if ((configNode["Type"].As<std::string>()).compare("HyperLogLog") == 0) {
        //
        //      auto myHLL = hyperLogLog::initialize( /*physicalSourcePtr,*/ field, duration, interval, configNode);
        //      statCollector* statCollectorPtr = myHLL;
        //      return statCollectorPtr;
        //
        //    } else if ((configNode["Type"].As<std::string>()).compare("ReservoirSample") == 0) {
        //
        //      auto mySample = reservoirSample::initialize( /*physicalSourcePtr,*/ field, duration, interval, configNode);
        //      statCollector* statCollectorPtr = mySample;
        //      return statCollectorPtr;

//      }
    } else {

      std::cout << "statCollector type is unknown!\n Returning nullptr!" << std::endl;
      return nullptr;

    }
  }

} // NES