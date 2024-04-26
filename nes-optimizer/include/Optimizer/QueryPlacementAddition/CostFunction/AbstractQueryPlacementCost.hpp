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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_COSTFUNCTION_ABSTRACTQUERYPLACEMENTCOST_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_COSTFUNCTION_ABSTRACTQUERYPLACEMENTCOST_HPP_

#include <memory>
#include <vector>

namespace NES {

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

/**
 * @brief Represents a matrix used for operator placement.
 * This matrix is a 2D vector of bools where each element indicates
 * whether a particular operator is placed at a specific position
 * (row, column). `true` means an operator is placed at that topology node,
 * while `false` means the operator is not placed in that topology node.
 * The matrix assumes the operator and nodes are sorted using depth-first iterator.
 */
using PlacementMatrix = std::vector<std::vector<bool>>;

namespace Optimizer {
class AbstractQueryPlacementCost {
  public:
    AbstractQueryPlacementCost();

    /**
     * @brief compute the estimated cost of applying the given placement candidate
     * @return cost of the placement candidate
     */
    virtual float getCost(PlacementMatrix candidate) = 0;
};
} // namespace Optimizer
} // namespace NES

#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_COSTFUNCTION_ABSTRACTQUERYPLACEMENTCOST_HPP_
