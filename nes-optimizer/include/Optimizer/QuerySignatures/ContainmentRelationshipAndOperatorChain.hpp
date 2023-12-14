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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYSIGNATURES_CONTAINMENTRELATIONSHIPANDOPERATORCHAIN_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYSIGNATURES_CONTAINMENTRELATIONSHIPANDOPERATORCHAIN_HPP_

#include <memory>
#include <vector>

namespace NES {

class LogicalOperator;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperator>;

namespace Optimizer {

class ContainmentRelationshipAndOperatorChain;
using ContainmentRelationshipAndOperatorChainPtr = std::unique_ptr<ContainmentRelationshipAndOperatorChain>;

/**
 * @brief enum describing the given containment relationship
 */
enum class ContainmentRelationship : uint8_t { NO_CONTAINMENT, LEFT_SIG_CONTAINED, RIGHT_SIG_CONTAINED, EQUALITY };

/**
* @brief this class stores the containment relationship and any extracted operator chains for TD-CQM and BU-CQM
*/
class ContainmentRelationshipAndOperatorChain {

  public:
    static ContainmentRelationshipAndOperatorChainPtr create(ContainmentRelationship containmentRelationship,
                                                             std::vector<LogicalOperatorNodePtr> containedOperatorChain);

    ContainmentRelationship containmentRelationship;
    std::vector<LogicalOperatorNodePtr> containedOperatorChain;

  private:
    explicit ContainmentRelationshipAndOperatorChain(ContainmentRelationship containmentRelationship,
                                                     std::vector<LogicalOperatorNodePtr> containedOperatorChain);
};
}// namespace Optimizer
}// namespace NES

#endif  // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYSIGNATURES_CONTAINMENTRELATIONSHIPANDOPERATORCHAIN_HPP_
