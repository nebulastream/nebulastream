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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTIC_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTIC_HPP_

#include <Operators/LogicalOperators/StatisticCollection/Characteristic/Characteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/StatisticValue.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
namespace NES::Statistic {

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;

/**
 * @brief This class acts as the parent class for all statistics, e.g., synopses or any other statistic
 */
class Statistic : public std::enable_shared_from_this<Statistic>{
  public:
    /**
     * @brief Constructor for a Statistic
     * @param startTs
     * @param endTs
     * @param observedTuples
     */
    Statistic(const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs, uint64_t observedTuples);

    /**
     * @brief Returns the statistic value for this statistic, i.e., a selectivity of 0.5
     * @param characteristic: The characteristic, e.g., Selectivity("f1" > 4)
     * @return
     */
    [[nodiscard]] virtual StatisticValue<> getStatisticValue(const Characteristic& characteristic) const = 0;

    /**
     * @brief Checks for equality
     * @param other
     * @return True, if equal, false otherwise
     */
    virtual bool equal(const Statistic& other) const = 0;

    /**
     * @brief Checks for equality
     * @param other
     * @return True, if equal, false otherwise
     */
    bool operator==(const Statistic& other) const;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    virtual std::string toString() const = 0;

    /**
     * @brief Checks if the current Characteristic is of type CharacteristicType
     * @tparam Characteristic
     * @return bool true if node is of Characteristic
     */
    template<class Characteristic>
    bool instanceOf() {
        if (dynamic_cast<Characteristic*>(this)) {
            return true;
        }
        return false;
    };

    /**
     * @brief Checks if the current Statistic is of type const StatisticType
     * @tparam Statistic
     * @return bool true if node is of Statistic
     */
    template<class Statistic>
    bool instanceOf() const {
        if (dynamic_cast<const Statistic*>(this)) {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the characteristic to a StatisticType
    * @tparam StatisticType
    * @return returns a shared pointer of the StatisticType
    */
    template<class StatisticType>
    std::shared_ptr<StatisticType> as() {
        if (instanceOf<StatisticType>()) {
            return std::dynamic_pointer_cast<StatisticType>(this->shared_from_this());
        }
        throw std::logic_error("We performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(StatisticType).name());
    }

    /**
    * @brief Dynamically casts the characteristic to a StatisticType
    * @tparam StatisticType
    * @return returns a shared pointer of the StatisticType
    */
    template<class StatisticType>
    std::shared_ptr<StatisticType> as() const {
        if (instanceOf<StatisticType>()) {
            return std::dynamic_pointer_cast<StatisticType>(this->shared_from_this());
        }
        throw std::logic_error("We performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(StatisticType).name());
    }

    /**
     * @brief Getter for the startTs
     * @return Windowing::TimeMeasure
     */
    Windowing::TimeMeasure getStartTs() const;

    /**
     * @brief Getter for the endTs
     * @return Windowing::TimeMeasure
     */
    Windowing::TimeMeasure getEndTs() const;

    /**
     * @brief Getter for the observedTuples
     * @return uint64_t
     */
    uint64_t getObservedTuples() const;

    /**
     * @brief virtual destructor
     */
    virtual ~Statistic();

  protected:
    Windowing::TimeMeasure startTs;
    Windowing::TimeMeasure endTs;
    uint64_t observedTuples;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTIC_HPP_
