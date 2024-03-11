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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ABSTRACTCOSTBASEDPLACEMENTADDITIONSTRATEGY_HPP
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ABSTRACTCOSTBASEDPLACEMENTADDITIONSTRATEGY_HPP

#include <memory>
#include <vector>

#include <Optimizer/QueryPlacementAddition/CostFunction/AbstractQueryPlacementCost.hpp>
#include <set>

namespace NES::Optimizer {
/**
 * A placement optimizer is only responsible for mathematical optimization of a given candidate.
 */
class AbstractCostBasedOptimizer {
  public:
    AbstractCostBasedOptimizer(const std::set<LogicalOperatorPtr>& pinnedUpstreamOperators,
                               const std::set<LogicalOperatorPtr>& pinnedDownstreamOperators);

    /**
     * @brief search for placement candidate with minimum cost.
     * @return a placement matrix of a candidate
     */
    virtual PlacementMatrix optimize() = 0;

  protected:
    std::set<LogicalOperatorPtr> pinnedUpstreamOperators;
    std::set<LogicalOperatorPtr> pinnedDownstreamOperators;
};

}// namespace NES::Optimizer

#endif//NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ABSTRACTCOSTBASEDPLACEMENTADDITIONSTRATEGY_HPP
