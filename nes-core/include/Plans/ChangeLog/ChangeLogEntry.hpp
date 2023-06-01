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

#ifndef NES_CHANGELOGENTRY_HPP
#define NES_CHANGELOGENTRY_HPP

#include <memory>
#include <set>

namespace NES {
class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;
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

    static ChangeLogEntryPtr create(std::set<OperatorNodePtr> upstreamOperators, std::set<OperatorNodePtr> downstreamOperators);

    // Impacted upstream operators
    const std::set<OperatorNodePtr> upstreamOperators;
    // Impacted downstream operators
    const std::set<OperatorNodePtr> downstreamOperators;

  private:
    ChangeLogEntry(std::set<OperatorNodePtr> upstreamOperators, std::set<OperatorNodePtr> downstreamOperators);
};
}// namespace NES::Optimizer::Experimental
#endif//NES_CHANGELOGENTRY_HPP
