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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICKEY_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICKEY_HPP_

#include <StatisticCollection/Characteristic/Characteristic.hpp>

namespace NES::Statistic {

/**
 * @brief This class represents how to uniquely identify a statistic in the system
 */
class StatisticKey {
  public:
    /**
     * @brief Constructor for a StatisticKey
     * @param characteristic
     * @param window
     */
    StatisticKey(const CharacteristicPtr& characteristic, const Windowing::WindowTypePtr& window);

    /**
     * @brief Getter for the characteristic
     * @return CharacteristicPtr
     */
    CharacteristicPtr getCharacteristic() const;

    /**
     * @brief Getter for the window
     * @return WindowTypePtr
     */
    Windowing::WindowTypePtr getWindow() const;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const StatisticKey& rhs) const;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if NOT equal otherwise false
     */
    bool operator!=(const StatisticKey& rhs) const;

    /**
     * @brief Calculates the hash
     * @return std::size_t
     */
    std::size_t hash() const;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() const;

  private:
    const CharacteristicPtr characteristic;
    const Windowing::WindowTypePtr window;
};

/**
 * @brief Necessary for unordered_map with StatisticKey as the key
 */
struct StatisticKeyHash {
    std::size_t operator()(const StatisticKey& key) const { return key.hash(); }
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICKEY_HPP_
