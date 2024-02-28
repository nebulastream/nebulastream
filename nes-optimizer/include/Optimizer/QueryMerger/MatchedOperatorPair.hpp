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

#include <Optimizer/QuerySignatures/ContainmentRelationshipAndOperatorChain.hpp>
#include <memory>
#include <vector>

namespace NES {

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

namespace Optimizer {

class MatchedOperatorPair;
using MatchedOperatorPairPtr = std::unique_ptr<MatchedOperatorPair>;

/**
 * @brief this class stores a pair of matched operators. One from the host shared query plan and one from the target shared query plan.
 * Addionally, it stores the containment relationship between the two operators.
*/
class MatchedOperatorPair {

  public:
    static MatchedOperatorPairPtr create(LogicalOperatorPtr hostOperator,
                                         LogicalOperatorPtr targetOperator,
                                         Optimizer::ContainmentRelationship containmentRelationship);

    LogicalOperatorPtr hostOperator;
    LogicalOperatorPtr targetOperator;
    Optimizer::ContainmentRelationship containmentRelationship;

  private:
    explicit MatchedOperatorPair(LogicalOperatorPtr hostOperator,
                                 LogicalOperatorPtr targetOperator,
                                 Optimizer::ContainmentRelationship containmentRelationship);
};
}// namespace Optimizer
}// namespace NES
