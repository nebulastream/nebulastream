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

#ifndef NES_COORDINATOR_INCLUDE_UTIL_INCREMENTALPLACEMENTUTILS_HPP_
#define NES_COORDINATOR_INCLUDE_UTIL_INCREMENTALPLACEMENTUTILS_HPP_
#include <Identifiers.hpp>
#include <memory>
#include <set>
#include <vector>

namespace NES {

namespace Optimizer {
class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

using ExecutionNodeWLock = std::shared_ptr<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>;
}// namespace Optimizer

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

namespace Experimental {

/**
 * @brief identifies the upstream and downstream operators that can remain deployed on the same nodes as they were
 * before the topology change occurred. These sets can be used as input for an incremental placement. All operators
 * placed upstream of the set of upstream operators or downstream of the downstream operators can remain as is and
 * will not need to be re-placed
 * @param sharedQueryPlan the shared query id for which the up and downstream pinned operators need to be identified
 * @param lockedUpstreamNode the upstream node of the topology link that was removed
 * @param lockedDownstreamNode the downstream node of the topology link that was removed
 * @param topology a pointer to the topology
 * @return a pair constaining two sets of operator id in the order {UPSTREAM, DOWNSTREAM}
 */
std::pair<std::set<OperatorId>, std::set<OperatorId>>
findUpstreamAndDownstreamPinnedOperators(const SharedQueryPlanPtr& sharedQueryPlan,
                                         Optimizer::ExecutionNodeWLock lockedUpstreamNode,
                                         Optimizer::ExecutionNodeWLock lockedDownstreamNode,
                                         const TopologyPtr& topology);

/**
 * @brief find all pairs of network sinks and sources that connect the specified up- and downstream node and belong
 * to the specified shared query
 * @param sharedQueryPlanId the id of the shared query whose decomposed query plan are considered
 * @param lockedUpstreamNode the node hosting the network sinks
 * @param lockedDownstreamNode the node hosting the network sources
 * @return a vector source-sink-pairs in the format {SinkOperator, SourceOperator}
 */
std::vector<std::pair<LogicalOperatorNodePtr, LogicalOperatorNodePtr>>
findNetworkOperatorsForLink(const SharedQueryId& sharedQueryPlanId,
                            Optimizer::ExecutionNodeWLock lockedUpstreamNode,
                            Optimizer::ExecutionNodeWLock lockedDownstreamNode);

}// namespace Experimental
}// namespace NES
#endif // NES_COORDINATOR_INCLUDE_UTIL_INCREMENTALPLACEMENTUTILS_HPP_
