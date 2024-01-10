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

#ifndef NES_OPTIMIZER_INCLUDE_PLANS_CHANGELOG_CHANGELOGENTRY_HPP_
#define NES_OPTIMIZER_INCLUDE_PLANS_CHANGELOG_CHANGELOGENTRY_HPP_

#include <Identifiers.hpp>
#include <memory>
#include <set>

namespace NES {
class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;
}// namespace NES

namespace NES::Optimizer::Experimental {

class ChangeLogEntry;
using ChangeLogEntryPtr = std::shared_ptr<ChangeLogEntry>;

/**
 * @brief: This class stores individual entry within a change log of a shared query plan. Each entry represents, changes occurred
 * to a shared query plan after applying the query or topology updates (node removal, link removal, query addition, query removal).
 * This change can be visualized as a sub-query plan and is represented by the change log entry using sets of upstream and downstream operators.
 */
class ChangeLogEntry {

  public:
    /**
     * @param: Create change log entry
     * @param upstreamOperators: upstream operator set
     * @param downstreamOperators: downstream operator set
     * @return shared pointer to change log entry
     */
    static ChangeLogEntryPtr create(std::set<LogicalOperatorNodePtr> upstreamOperators,
                                    std::set<LogicalOperatorNodePtr> downstreamOperators);

    // Impacted upstream operators
    const std::set<LogicalOperatorNodePtr> upstreamOperators;
    // Impacted downstream operators
    const std::set<LogicalOperatorNodePtr> downstreamOperators;
    // A partially ordered set (poset) of operator ids that represent the sub-query plan captured in the changelog entry
    const std::set<OperatorId> poSetOfSubQueryPlan;

  private:
    ChangeLogEntry(std::set<LogicalOperatorNodePtr> upstreamOperators, std::set<LogicalOperatorNodePtr> downstreamOperators);

    /**
     * @brief: This method computes the partially ordered set of operator ids representing the sub-query plan captured by change log entry.
     * In particular, perform a BFS traversal from upstream to downstream operators of the sub-query plan.
     * @return set of operator ids
     */
    std::set<OperatorId> computePoSet();
};
}// namespace NES::Optimizer::Experimental
#endif // NES_OPTIMIZER_INCLUDE_PLANS_CHANGELOG_CHANGELOGENTRY_HPP_
