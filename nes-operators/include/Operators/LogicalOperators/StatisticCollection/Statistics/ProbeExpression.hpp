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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_PROBEEXPRESSION_PROBEEXPRESSION_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_PROBEEXPRESSION_PROBEEXPRESSION_HPP_

#include <Operators/Expressions/ExpressionNode.hpp>

namespace NES::Statistic {

/*
 * @brief This class is being used as a container to store a probeExpression. As we do not want to limit ourselves to
 * probeExpressions in the future, we create an abstract class so that we can easily extend the functionality.
 * To receive a StatisticValue from a Statistic, we pass an object of this class and then the Statistic has to evaluate
 * the expression and return a StatisticValue. The actual implementation highly depends on the underlying Statistic.
 * For example, a Count-Min sketch can only evaluate a ArithmeticalBinaryExpressionNode with one side being a
 * ConstantValueExpressionNode and the other expression the tracked field name. A sample is a little bit more powerful
 * in terms of what expression it can expect and evaluate, as it stores all fields of the data stream.
 */
class ProbeExpression {
  public:
    explicit ProbeExpression(const ExpressionNodePtr& probeExpression);

    const ExpressionNodePtr& getProbeExpression() const;

  private:
    ExpressionNodePtr probeExpression;
};

}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_PROBEEXPRESSION_PROBEEXPRESSION_HPP_
