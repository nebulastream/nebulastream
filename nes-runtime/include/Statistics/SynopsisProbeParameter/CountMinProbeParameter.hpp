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

#ifndef NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_COUNTMINPROBEPARAMETER_HPP_
#define NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_COUNTMINPROBEPARAMETER_HPP_

#include <Statistics/SynopsisProbeParameter/StatisticProbeParameter.hpp>

namespace NES {

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

namespace Experimental::Statistics {

/**
 * @brief the class of objects defining what data is to be probed from a countMin
 */
class CountMinProbeParameter : public StatisticProbeParameter {
  public:
    /**
     * @brief the constructor of the CountMinProbeParameter class that defines how a CountMin is probed and what kind of data is
     * extracted from the sketch
     * @param expression the expression defining what is to be probed
     */
    explicit CountMinProbeParameter(ExpressionNodePtr& expression);

    /**
     * @return returns the query type (for CountMin point or range)
     */
    [[nodiscard]] StatisticQueryType getQueryType() const;

    /**
     * @return returns a boolean, which defines for range queries whether the probe logic needs to be flipped
     */
    [[nodiscard]] bool isInverse() const;

    /**
     * @return returns the field name that is to be probed
     */
    [[nodiscard]] const std::string& getFieldName() const;

    /**
     * @return returns the constant value of the point or range query
     */
    [[nodiscard]] uint64_t getConstQueryValue() const;

  private:
    StatisticQueryType queryType;
    bool inverse;
    std::string fieldName;
    uint64_t constQueryValue;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_COUNTMINPROBEPARAMETER_HPP_
