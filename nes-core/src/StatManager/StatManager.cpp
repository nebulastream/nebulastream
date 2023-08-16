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

#include <Util/Logger/Logger.hpp>
#include <StatManager/StatManager.hpp>
#include <StatManager/StatCollectors/StatCollectorConfiguration.hpp>
#include <StatManager/StatCollectors/Synopses/Sketches/CountMin/CountMin.hpp>
#include <StatManager/StatCollectors/StatCollector.hpp>

namespace NES::Experimental::Statistics {

	std::vector<StatCollectorPtr>& StatManager::getStatCollectors() {
		return statCollectors;
	}

  void StatManager::createStat (const StatCollectorConfig& config) {
    auto physicalSourceName = config.getPhysicalSourceName();
    auto field = config.getField();
    auto duration = config.getDuration();
    auto frequency = config.getFrequency();

    bool found = false;

    // check if the stat is already being tracked passively
    for (const auto &collector: statCollectors) {
      // could add a hashmap or something here that first checks if a statCollector can even generate the according statistic
      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {
        found = true;
        break;
      }
    }

    auto statMethodName = config.getStatMethodName();

    // if the stat is not yet tracked, then create it
    if (!found) {
      if (statMethodName.compare("FrequencyCM")) {
        // get config values
        auto error = static_cast<double>(config.getError());
        auto probability = static_cast<double>(config.getProbability());
        auto depth = static_cast<uint32_t>(config.getDepth());
        auto width = static_cast<uint32_t>(config.getWidth());

        // check that only error and probability or depth and width are set
        if (((depth != 0 && width != 0) && (error == 0.0 && probability == 0.0)) ||
            ((depth == 0 && width == 0) && (error != 0.0 && probability != 0.0))) {
          // check if error and probability are set and within valid value ranges and if so, create a CM Sketch
          if (error != 0.0 && probability != 0.0) {
            if (error <= 0 || error >= 1) {
              NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.");

            } else if (probability <= 0 || probability >= 1) {
              NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.");

            } else {
              // create stat
              std::unique_ptr<CountMin> cm = std::make_unique<CountMin>(config);

              statCollectors.push_back(std::move(cm));

            }
          }
        }
      // TODO: Potentially add more Synopses here dependent on Nautilus integration.
      } else {
        NES_DEBUG("statMethodName is not defined!");
      }
    }
  }


  double StatManager::queryStat (const StatCollectorConfig& config,
                                 const uint32_t key) {
    auto physicalSourceName = config.getPhysicalSourceName();
    auto field = config.getField();
    auto duration = static_cast<time_t>(config.getDuration());
    auto frequency = static_cast<time_t>(config.getFrequency());

    // check if the stat is already being tracked passively
    for (const auto& collector : statCollectors) {
      // could add a hashmap or something here that first checks if a statCollector can even generate the according statistic
      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {
        auto statMethodName = config.getStatMethodName();

        if (statMethodName.compare("FrequencyCM")) {
          auto cm = dynamic_cast<CountMin*>(collector.get());
          return cm->pointQuery(key);
        } else {
          NES_DEBUG("No compatible combination of statistic and method for querying of statCollector!");
        }
      }
    }
    return -1.0;
  }

  void StatManager::deleteStat(const StatCollectorConfig& config) {
    auto physicalSourceName = config.getPhysicalSourceName();
    auto field = config.getField();
    auto duration = config.getDuration();
    auto frequency = config.getFrequency();
    auto statMethodName = config.getStatMethodName();

    for (auto it = statCollectors.begin(); it != statCollectors.end(); it++) {
      auto& collector = *it;

      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {
        if (statMethodName.compare("FrequencyCM")) {
          statCollectors.erase(it);
          return;

          } else {
            NES_DEBUG("Could not delete Statistic Object!");
            return;
        }
      }
    }
    NES_DEBUG("Could not delete Statistic Object!");
    return;
  }

} // NES::Experimental::Statistics
