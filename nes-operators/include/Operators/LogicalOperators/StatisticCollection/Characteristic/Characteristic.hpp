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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/Metric.hpp>
namespace NES::Statistic {
class Characteristic;
using CharacteristicPtr = std::shared_ptr<Characteristic>;

/**
 * @brief Parent class for all different statistic characteristic, e.g., workload, data, infrastructure.
 */
class Characteristic : public std::enable_shared_from_this<Characteristic> {
  public:
    /**
     * @brief Constructor for a Characteristic
     * @param type
     */
    explicit Characteristic(MetricPtr type) : type(std::move(type)) {}

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
     * @brief Checks if the current Characteristic is of type const CharacteristicType
     * @tparam Characteristic
     * @return bool true if node is of Characteristic
     */
    template<class Characteristic>
    bool instanceOf() const {
        if (dynamic_cast<const Characteristic*>(this)) {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the characteristic to a CharacteristicType
    * @tparam CharacteristicType
    * @return returns a shared pointer of the CharacteristicType
    */
    template<class CharacteristicType>
    std::shared_ptr<CharacteristicType> as() {
        if (instanceOf<CharacteristicType>()) {
            return std::dynamic_pointer_cast<CharacteristicType>(this->shared_from_this());
        }
        throw std::logic_error("We performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(CharacteristicType).name());
    }

    /**
    * @brief Dynamically casts the characteristic to a CharacteristicType
    * @tparam CharacteristicType
    * @return returns a shared pointer of the CharacteristicType
    */
    template<class CharacteristicType>
    std::shared_ptr<CharacteristicType> as() const {
        if (instanceOf<CharacteristicType>()) {
            return std::dynamic_pointer_cast<CharacteristicType>(this->shared_from_this());
        }
        throw std::logic_error("We performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(CharacteristicType).name());
    }

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    virtual bool operator==(const Characteristic& rhs) const { return (*type) == (*rhs.type); };

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if NOT equal otherwise false
     */
    virtual bool operator!=(const Characteristic& rhs) const { return !(*this == rhs); };

    /**
     * @brief Computes a hash for this characteristic
     * @return std::size_t
     */
    virtual std::size_t hash() const = 0;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    virtual std::string toString() const = 0;

    /**
     * @brief Virtual deconstructor
     */
    virtual ~Characteristic() = default;

    /**
     * @brief Getter for the metric type
     * @return MetricPtr
     */
    [[nodiscard]] MetricPtr getType() const { return type; }

  protected:
    MetricPtr type;
};

}// namespace NES::Statistic
#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_HPP_
