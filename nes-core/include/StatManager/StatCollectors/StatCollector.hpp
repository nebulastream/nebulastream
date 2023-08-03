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
#include <Configurations/StatManagerConfiguration.hpp>

namespace NES {

  class StatCollector {
  public:
    const std::string& getPhysicalSourceName() const;
    const std::string& getField() const;
    time_t getDuration() const;
    time_t getFrequency() const;
    std::vector<uint32_t>& getKeys();
    void addKey(uint32_t key);
    StatCollector(const Configurations::StatManagerConfig config);
//    static StatCollector* createStat(const std::string& physicalSourceName,
//        const std::string& field,
//        time_t duration,
//        time_t interval,
//        Yaml::Node configNode
//    );
    virtual void update(uint32_t key) = 0;
    virtual bool equal(StatCollector* rightStatCollector, bool statCollection) = 0;
    virtual StatCollector* merge(StatCollector* rightStatCollector, bool statCollection) = 0;
//    virtual void update(uint64_t) = 0;
//    virtual void destroy() = 0;

  private:
    const std::string physicalSourceName;
    const std::string field;
    const time_t duration;
    const time_t frequency;
//    std::vector<double> keys;
  };

} // NES

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_STATCOLLECTOR_HPP
