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
#include "StatManager/StatCollectors/Synopses/Sketches/CountMin/CountMin.hpp"

namespace NES {

  std::vector<NES::StatCollector*>& StatManager::getStatCollectors() {
    return statCollectors;
  }

  void StatManager::createStat (const Configurations::StatManagerConfig& config) {

    auto physicalSourceName = config.physicalSourceName.getValue();
    auto field = config.field.getValue();
    auto duration = static_cast<time_t>(config.duration.getValue());
    auto frequency = static_cast<time_t>(config.frequency.getValue());

    bool found = 0;

    // check if the stat is already being tracked passively
    for (const auto& collector : statCollectors) {
      // could add a hashmap or something here that first checks if a statCollector can even generate the according statistic
      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {

        found = 1;

        break;
      }
    }

    auto statMethodName = config.statMethodName.getValue();

//    // if the stat is not yet tracked, then create it
//    if(!found) {

    if (statMethodName.compare("FrequencyCM")) {

      // get config values
      auto error = static_cast<double>(config.error.getValue());
      auto probability = static_cast<double>(config.probability.getValue());
      auto depth = static_cast<uint32_t>(config.depth.getValue());
      auto width = static_cast<uint32_t>(config.width.getValue());

      // check that only error and probability or depth and width are set
      if (((depth != 0 && width != 0) && (error == 0.0 && probability == 0.0)) ||
          ((depth == 0 && width == 0) && (error != 0.0 && probability != 0.0))) {

        // check if error and probability are set and within valid value ranges and if so, create a CM Sketch
        if (error != 0.0 && probability != 0.0) {

          if (error <= 0 || error >= 1) {

            NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.")

          } else if (probability <= 0 || probability >= 1) {

            NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.");

          } else {

            // create stat
            auto cm = new CountMin(config);

            // write stat to statVector
            StatCollector* cmSketch = cm;
            statCollectors.push_back(cmSketch);
          }



        }
      }

//      } else if (statMethodName.compare("Distinct Keys")) {

//      } else if (statMethodName.compare("Quantile")) {

    } else {
      NES_DEBUG("statMethodName is not defined!");
    }
  }


  double StatManager::queryStat (const Configurations::StatManagerConfig& config,
                                 const uint32_t key) {

    auto physicalSourceName = config.physicalSourceName.getValue();
    auto field = config.field.getValue();
    auto duration = static_cast<time_t>(config.duration.getValue());
    auto frequency = static_cast<time_t>(config.frequency.getValue());

    // check if the stat is already being tracked passively
    for (const auto& collector : statCollectors) {
      // could add a hashmap or something here that first checks if a statCollector can even generate the according statistic
      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {

        auto statMethodName = config.statMethodName.getValue();

        if (statMethodName.compare("FrequencyCM")) {
          auto cm = dynamic_cast<CountMin*>(collector);
          return cm->pointQuery(key);
        } else {
          NES_DEBUG("No compatible combination of statistic and method for querying of statCollector!");
        }
      }
    }
    return -1.0;
  }

  void StatManager::deleteStat(const Configurations::StatManagerConfig& config) {

    auto physicalSourceName = config.physicalSourceName.getValue();
    auto field = config.field.getValue();
    auto duration = static_cast<time_t>(config.duration.getValue());
    auto frequency = static_cast<time_t>(config.frequency.getValue());
    auto statMethodName = config.statMethodName.getValue();

    for (auto it = statCollectors.begin(); it != statCollectors.end(); it++) {

      auto collector = *it;

      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {

        if (statMethodName.compare("FrequencyCM")) {

          statCollectors.erase(it);
          return;

//        } else if (statMethodName.compare("FrequencyRS")) {
//
//        } else if (statMethodName.compare("QuantileDD")) {
//
          } else {
            NES_DEBUG("Could not delete Statistic Object!");
            return;
        }
      }
    }
    NES_DEBUG("Could not delete Statistic Object!");
    return;
  }

//  void deleteStat (const std::string& physicalSourceName,
//                   const std::string& field,
//                   const std::string& statName) {
//
//  }

} // NES

