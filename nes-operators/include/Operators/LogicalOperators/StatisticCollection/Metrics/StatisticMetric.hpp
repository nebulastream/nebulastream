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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_METRICS_STATISTICMETRIC_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_METRICS_STATISTICMETRIC_HPP_

#include <StatisticIdentifiers.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <utility>

namespace NES::Statistic {
class StatisticMetric;
using MetricPtr = std::shared_ptr<StatisticMetric>;

/**
 * @brief The metric hash is the hash corresponding to a specific metric. We use this in combination with the
 * StatisticId to uniquely identify a statistic in our system. By using the StatisticMetricHash, we do not have to send the whole
 * metric down to the physical operator but can rather send the StatisticMetricHash. In the physical operator, we receive the
 * statistic id as part of the tuple buffer and therefore, we can uniquely identify a statistic
 */
using StatisticMetricHash = uint64_t;

/**
 * @brief This class acts as an abstract class for all possible statistic types, e.g., Cardinality over field f1
 */
class StatisticMetric : public std::enable_shared_from_this<StatisticMetric> {
  public:
    /**
     * @brief Constructor for a Metric object
     * @param field
     */
    explicit StatisticMetric(FieldAccessExpressionNodePtr field);

    /**
     * @brief Gets the field over which the Metric should be collected/built
     * @return FieldAccessExpressionNodePtr
     */
    FieldAccessExpressionNodePtr getField() const;

    /**
     * @brief Calculates a hash of the underlying metric
     * @return StatisticMetricHash
     */
    StatisticMetricHash hash() const;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    virtual bool operator==(const StatisticMetric& rhs) const = 0;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    virtual bool equal(const StatisticMetric&) const;

    /**
     * @brief Checks if the current Metric is of type MetricType
     * @tparam StatisticMetricType
     * @return bool true if node is of Metric
     */
    template<class StatisticMetricType>
    bool instanceOf() {
        if (dynamic_cast<StatisticMetricType*>(this)) {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the node to a MetricType
    * @tparam StatisticMetricType
    * @return returns a shared pointer of the MetricType
    */
    template<class StatisticMetricType>
    std::shared_ptr<StatisticMetricType> as() {
        if (instanceOf<StatisticMetricType>()) {
            return std::dynamic_pointer_cast<StatisticMetricType>(this->shared_from_this());
        }
        throw std::logic_error("We performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(StatisticMetricType).name());
    }

    /**
     * @brief Checks if the current Metric is of type const MetricType
     * @tparam StatisticMetricType
     * @return bool true if node is of Metric
     */
    template<class StatisticMetricType>
    [[nodiscard]] bool instanceOf() const {
        if (dynamic_cast<const StatisticMetricType*>(this)) {
            return true;
        }
        return false;
    };

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    virtual std::string toString() const = 0;

    /**
     * @brief Virtual destructor
     */
    virtual ~StatisticMetric() = default;

  protected:
    const FieldAccessExpressionNodePtr field;
};

/**
 * @brief Creates a FieldAccessExpressionNode for the field name
 * @param name
 * @return FieldAccessExpressionNodePtr
 */
FieldAccessExpressionNodePtr Over(std::string name);

}// namespace NES::Statistic
#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_METRICS_STATISTICMETRIC_HPP_
