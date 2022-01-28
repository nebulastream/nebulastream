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

#ifndef NES_INCLUDE_PLANS_GLOBAL_QUERY_SHAREDQUERYPLANCHANGELOG_HPP_
#define NES_INCLUDE_PLANS_GLOBAL_QUERY_SHAREDQUERYPLANCHANGELOG_HPP_

#include <map>
#include <memory>
#include <vector>

namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class SharedQueryPlanChangeLog;
using SharedQueryPlanChangeLogPtr = std::shared_ptr<SharedQueryPlanChangeLog>;

/**
 * SharedQueryPlanChangeLog records the changes occurred on a shared query plan after its creation.
 * NOTE: The change log will not contain anything when the SharedQueryPlan is created or when the entire query plan is removed.
 */
class SharedQueryPlanChangeLog {

  public:
    ~SharedQueryPlanChangeLog() = default;

    /**
     * Create instance of SharedQueryPlanChangeLog
     * @return SharedQueryPlanChangeLog ptr
     */
    static SharedQueryPlanChangeLogPtr create();

    /**
     * Add information about the addition of new operator
     * @param upstreamOperator: the upstream operator to which the operator is added
     * @param addedOperatorId: the operator id of the new operator
     */
    void addAddition(const OperatorNodePtr& upstreamOperator, uint64_t addedOperatorId);

    /**
     * Add information about the removal of operator
     * @param upstreamOperator : the upstream operator from which the operator is removed
     * @param removedOperatorId : the id of the removed operator
     */
    void addRemoval(const OperatorNodePtr& upstreamOperator, uint64_t removedOperatorId);

    /**
     * Register newly added sink
     * @param sinkOperatorId: id of the sink operator
     */
    void registerNewlyAddedSink(uint64_t sinkOperatorId);

    /**
     * Register removed sink
     * @param sinkOperatorId: id of the sink operator
     */
    void registerRemovedSink(uint64_t sinkOperatorId);

    /**
     * Get the addition log
     * @return the map containing all addition changes
     */
    [[nodiscard]] const std::map<OperatorNodePtr, std::vector<uint64_t>>& getAddition() const;

    /**
     * Get removal log
     * @return the map containing all removal changes
     */
    [[nodiscard]] const std::map<OperatorNodePtr, std::vector<uint64_t>>& getRemoval() const;

    /**
     * Get newly added sinks
     * @return list of sink operator ids
     */
    [[nodiscard]] const std::vector<uint64_t>& getAddedSinks() const;

    /**
     * Get removed sinks
     * @return list of sink operator ids
     */
    [[nodiscard]] const std::vector<uint64_t>& getRemovedSinks() const;

    /**
     * clear addition log
     */
    void clearAdditionLog();

    /**
     * clear removal log
     */
    void clearRemovalLog();

  private:
    SharedQueryPlanChangeLog() = default;
    std::map<OperatorNodePtr, std::vector<uint64_t>> addition;
    std::map<OperatorNodePtr, std::vector<uint64_t>> removal;
    std::vector<uint64_t> addedSinks;
    std::vector<uint64_t> removedSinks;
};
}// namespace NES

#endif  // NES_INCLUDE_PLANS_GLOBAL_QUERY_SHAREDQUERYPLANCHANGELOG_HPP_
