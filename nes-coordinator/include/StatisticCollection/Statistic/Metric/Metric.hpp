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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICTYPE_STATISTICTYPE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICTYPE_STATISTICTYPE_HPP_
#include <API/Query.hpp>
#include <StatisticIdentifiers.hpp>
namespace NES::Statistic {
class Metric;
using MetricPtr = std::shared_ptr<Metric>;

/**
 * @brief This class acts as an abstract class for all possible statistic types
 */
class Metric {
  public:
    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    virtual bool operator==(const Metric&) const = 0;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if NOT equal otherwise false
     */
    virtual bool operator!=(const Metric& rhs) const { return !(*this == rhs); }

    /**
     * @brief Checks if the current Metric is of type MetricType
     * @tparam Metric
     * @return bool true if node is of Metric
     */
    template<class Metric>
    bool instanceOf() {
        if (dynamic_cast<Metric*>(this)) {
            return true;
        }
        return false;
    };

    /**
     * @brief Checks if the current Metric is of type const MetricType
     * @tparam Metric
     * @return bool true if node is of Metric
     */
    template<class Metric>
    [[nodiscard]] bool instanceOf() const {
        if (dynamic_cast<const Metric*>(this)) {
            return true;
        }
        return false;
    };

    virtual ~Metric() = default;
};
}// namespace NES::Statistic
#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICTYPE_STATISTICTYPE_HPP_
