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

#include <memory>

namespace NES {

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

namespace Optimizer {

class MatchedOperatorPair;
using MatchedOperatorPairPtr = std::unique_ptr<MatchedOperatorPair>;

/**
* @brief this class stores a pair of matched operators. One from the host shared query plan and one from the target shared query plan.
*/
class MatchedOperatorPair {

  public:
    static MatchedOperatorPairPtr create(LogicalOperatorNodePtr hostOperator, LogicalOperatorNodePtr targetOperator);

    LogicalOperatorNodePtr hostOperator;
    LogicalOperatorNodePtr targetOperator;

  private:
    explicit MatchedOperatorPair(LogicalOperatorNodePtr hostOperator, LogicalOperatorNodePtr targetOperator);
};
}// namespace Optimizer
}// namespace NES
