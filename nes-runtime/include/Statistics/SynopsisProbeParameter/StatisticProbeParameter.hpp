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

#ifndef NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_SYNOPSISPROBEPARAMETER_HPP_
#define NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_SYNOPSISPROBEPARAMETER_HPP_

#include <Statistics/SynopsisProbeParameter/StatisticQueryType.hpp>
#include <string>
#include <memory>

namespace NES {

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

namespace Experimental::Statistics {

/**
 * @brief the parent class of all statistic probe parameter classes which define how to specifically define the according data structure
 */
class StatisticProbeParameter {
  public:
    explicit StatisticProbeParameter() = default;

    virtual ~StatisticProbeParameter() = default;
};
using StatisticProbeParameterPtr = std::shared_ptr<StatisticProbeParameter>;
}// namespace Experimental::Statistics
}// namespace NES
#endif//NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_SYNOPSISPROBEPARAMETER_HPP_
