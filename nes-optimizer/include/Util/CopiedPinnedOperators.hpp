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

#ifndef NES_COPIEDPINNEDOPERATORS_HPP
#define NES_COPIEDPINNEDOPERATORS_HPP

#include <memory>
#include <queue>
#include <set>
#include <unordered_map>

namespace NES {

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

namespace Optimizer {

/**
 * @brief class to store copied pinned up and down stream operators
 */
class CopiedPinnedOperators {

  public:
    /**
     * @brief Create a CopiedPinnedOperators object
     * @param pinnedUpStreamOperators: the original pinned up stream operators
     * @param pinnedDownStreamOperators: the original pinned down stream operators
     * @param operatorIdToOriginalOperatorMap: the operator id to the original operator map
     * @return instance of CopiedPinnedOperators
     */
    static CopiedPinnedOperators create(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                        const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators,
                                        std::unordered_map<OperatorId, LogicalOperatorNodePtr>& operatorIdToOriginalOperatorMap);

    std::set<LogicalOperatorNodePtr> copiedPinnedUpStreamOperators;
    std::set<LogicalOperatorNodePtr> copiedPinnedDownStreamOperators;

  private:
    CopiedPinnedOperators(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                          const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators);
};
}// namespace Optimizer
}// namespace NES

#endif//NES_COPIEDPINNEDOPERATORS_HPP
