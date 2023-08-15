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
#ifndef NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HPP

#include <memory>
#include <StatManager/StatCollectors/StatCollector.hpp>

namespace NES::Experimental::Statistics {

  /**
   * @brief an abstract class that inherits from the general class statCollector, and further defines the more detailed
   * interface for all sketches.
   */
  class Sketch : public StatCollector {

  public:
    /**
     * @brief gets the depth of the sketch
     * @return depth
     */
    [[nodiscard]]uint32_t getDepth() const;

    /**
     * @brief gets the width of the sketch
     * @return width
     */
    [[nodiscard]]uint32_t getWidth() const;

    /**
     * @brief the constructor of the abstract class sketch
     * @param config a config object of type statCollectorConfig, which defines behavior such as the type of sketch, the
     * duration for which the statistic is to be collected, how ofter a statistic is to be collected, and more
     * @param depth the depth of the sketch
     * @param width the width of the sketch
     */
    Sketch(const StatCollectorConfig& config, const uint32_t depth, const uint32_t width);

    /**
     * @brief a pure virtual function that is inherited from the statCollector and which has to be defined by every sketch.
     * The update function modifies the sketch according to the key
     * @param key the key which modifies the sketch
     */
    void update(uint32_t key) override = 0;

    /**
     * @brief a pure virtual function, inherited from the statCollector, which needs to be implemented by every sketch.
     * equal() checks if two sketches are parametrized equally, which is especially important such that two or
     * more statCollectors can be merged. They do not need to be identical for true to be returned in terms of the
     * values in the sketch
     * @param rightStatCollector the other statCollector to be compared to
     * @param statCollection a boolean parameter, which tightens the conditions, whether two statCollectors are equal
     * or not. Within the context of statCollection, they of course need to also be build over the same duration,
     * frequency, and over the same field.
     * @return true if the two sketches are initialized similarly, such that they can be merged
     */
    bool equal(const StatCollectorPtr& rightSketch, bool statCollection) override = 0;

    /**
     * @brief a pure virtual function, inherited from the statCollector, which needs to be implemented by every sketch.
     * It checks whether the two sketches are constructed in the same manner (equal()), and if so combines the two
     * @param rightStatCollector the second statCollector
     * @param statCollection a boolean parameter, which tightens the conditions, whether two statCollectors are equal
     * or not. Within the context of statCollection, they of course need to also be build over the same duration,
     * frequency, and over the same field.
     * @return a unique statCollectorPtr to the results of the two merged statCollectors.
     */
    StatCollectorPtr merge(StatCollectorPtr rightSketch, bool statCollection) override = 0;

  private:
    uint32_t depth;
    uint32_t width;
  };

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HPP
