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

  class StatCollector;
  using StatCollectorPtr = std::unique_ptr<StatCollector>;

  /**
   * @brief this class implements the interface for StatCollectors, meaning arbitrary objects which can generate statistics
   */
  class StatCollector {
    public:


      virtual ~StatCollector() = default;

      /**
       * @brief gets the physicalSourceName over which the statistic object is constructed
       * @return physicalSourceName
       */
      [[nodiscard]] const std::string& getLogicalSourceName() const;

      /**
       * @brief gets the physicalSourceName over which the statistic object is constructed
       * @return physicalSourceName
       */
      [[nodiscard]] const std::string& getPhysicalSourceName() const;

      /**
       * @brief gets the field over which the statistic object is constructed
       * @return field
       */
      [[nodiscard]] const std::string& getField() const;

      /**
       * @brief gets the duration/time period over which the statistic object is constructed
       * @return duration
       */
      [[nodiscard]] time_t getDuration() const;

      /**
       * @brief gets the frequency/regular interval in which the statistic object is updated
       * @return frequency
       */
      [[nodiscard]] time_t getFrequency() const;

      /**
       * @brief The constructor of a StatCollector object
       * @param config The constructor takes a StatCollectorConfig object, which defines parameters such as the
       * physicalStreamName and Field over which a statistic is constructed, with which method to construct the statistic,
       * how long and in which intervals to construct the statistic and potentially more
       * @return an unique StatCollectorPtr
       */
      StatCollector(const StatCollectorConfig config);

      /**
       * @brief a pure virtual function which needs to be implemented by every kind of a statCollector. The update function
       * receives a key and has to update the statistic accordingly.
       * @param key The key for which a statistic is updated.
       */
      virtual void update(uint32_t key) = 0;

      /**
       * @brief a pure virtual function which needs to be implemented by every kind of a statCollector. equal() checks
       * if two statCollectors are parametrized equally, which is especially important such that two or more statCollectors
       * can be merged. They do not need to be purely identical for true to be returned.
       * @param rightStatCollector the other statCollector to be compared to
       * @param statCollection a boolean parameter, which tightens the conditions, whether two statCollectors are equal
       * or not. Within the context of statCollection, they of course need to also be build over the same duration,
       * frequency, and over the same field.
       * @return true if the two statCollectors are initialized similarly, such that they can be merged
       */
      virtual bool equal(const StatCollectorPtr& rightStatCollector, bool statCollection) = 0;

      /**
       * @brief a pure virtual function which needs to be implemented by every kind of a statCollector. It checks whether
       * the two statCollectors are constructed in the same manner (equal()), and if so combines the two
       * @param rightStatCollector the second statCollector
       * @param statCollection a boolean parameter, which tightens the conditions, whether two statCollectors are equal
       * or not. Within the context of statCollection, they of course need to also be build over the same duration,
       * frequency, and over the same field.
       * @return a unique statCollectorPtr to the results of the two merged statCollectors.
       */
      virtual StatCollectorPtr merge(StatCollectorPtr rightStatCollector, bool statCollection) = 0;

    private:
      const std::string logicalSourceName;
      const std::string physicalSourceName;
      const std::string field;
      // TODO: rename to window size
      const time_t duration;
      // TODO: rename to slide factor
      const time_t frequency;
  };

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_STATCOLLECTOR_HPP
