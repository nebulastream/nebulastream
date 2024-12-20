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

#pragma once

#include <memory>
#include <string>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Types/WindowType.hpp>

namespace NES
{
/**
 * This class adds the logical operators to the queryPlan and handles further conditions and updates on the updated queryPlan and its nodes, e.g.,
 * update the consumed sources after a binary operator or adds window characteristics to the join operator.
 */
class QueryPlanBuilder
{
public:
    /// Creates a query plan from a particular source. The source is identified by its name.
    /// During query processing the underlying source descriptor is retrieved from the source catalog.
    static QueryPlanPtr createQueryPlan(std::string logicalSourceName);

    /**
      * @brief this call projects out the attributes in the parameter list
      * @param functions list of attributes
      * @param queryPlan the queryPlan to add the projection node
      * @return the updated queryPlan
      */
    static QueryPlanPtr addProjection(const std::vector<NodeFunctionPtr>& functions, QueryPlanPtr queryPlan);

    /**
     * @brief this call add the rename operator to the queryPlan, this operator renames the source
     * @param newSourceName source name
     * @param queryPlan the queryPlan to add the rename node
     * @return the updated queryPlan
     */
    static QueryPlanPtr addRename(std::string const& newSourceName, QueryPlanPtr queryPlan);

    /**
     * @brief: this call add the selection operator to the queryPlan, the operator selections records according to the predicate. An
     * exemplary usage would be: selection(Attribute("f1" < 10))
     * @param selectionFunction as function node containing the predicate
     * @param queryPlanPtr the queryPlan the selection node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addSelection(NodeFunctionPtr const& selectionFunction, QueryPlanPtr queryPlan);

    /**
     * @brief: this call add the sort buffer operator to the queryPlan, the operator sorts the records in a buffer according
     * to the given field and order.
     * @param sortFieldIdentifier field identifier to sort by
     * @param sortOrder sort order
     * @param queryPlanPtr the queryPlan the sort buffer node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addSortBuffer(std::string const& sortFieldIdentifier, std::string const& sortOrder, QueryPlanPtr queryPlan);

    /**
     * @brief: this call add the delay buffer operator to the queryPlan, the operator delays some buffers according
     * to the given unorderedness.
     * @param queryPlanPtr the queryPlan the sort buffer node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addDelayBuffer(QueryPlanPtr queryPlan);

    /**
     * @brief: this call add the delay tuples operator to the queryPlan, the operator delays some tuples within a buffer according
     * to the given unorderedness.
     * @param queryPlanPtr the queryPlan the sort tuples node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addDelayTuples(QueryPlanPtr queryPlan);

    /**
     * @brief: this call adds the limit operator to the queryPlan, the operator limits the number of produced records.
     * @param filterFunction as function node containing the predicate
     * @param queryPlanPtr the queryPlan the filter node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addLimit(const uint64_t limit, QueryPlanPtr queryPlan);

    /**
     * @brief: Map records according to a map function. An
     * exemplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
     * @param mapFunction as function node
     * @param queryPlan the queryPlan the map is added to
     * @return the updated queryPlanPtr
     */
    static QueryPlanPtr addMap(NodeFunctionFieldAssignmentPtr const& mapFunction, QueryPlanPtr queryPlan);

    static QueryPlanPtr addWindowAggregation(
        QueryPlanPtr queryPlan,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        const std::vector<std::shared_ptr<Windowing::WindowAggregationDescriptor>>& windowAggs,
        const std::vector<std::shared_ptr<NodeFunctionFieldAccess>>& onKeys);

    /**
    * @brief UnionOperator to combine two query plans
    * @param leftQueryPlan the left query plan to combine by the union
    * @param rightQueryPlan the right query plan to combine by the union
    * @return the updated queryPlan combining left and rightQueryPlan with union
    */
    static QueryPlanPtr addUnion(QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan);

    /**
     * @brief This methods add the join operator to a query
     * @param leftQueryPlan the left query plan to combine by the join
     * @param rightQueryPlan the right query plan to combine by the join
     * @param joinFunction set of join Functions
     * @param windowType Window definition.
     * @return the updated queryPlan
     */
    static QueryPlanPtr addJoin(
        QueryPlanPtr leftQueryPlan,
        QueryPlanPtr rightQueryPlan,
        NodeFunctionPtr joinFunction,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        Join::LogicalJoinDescriptor::JoinType joinType);

    /// @note In contrast to joinWith(), batchJoinWith() does not require a window to be specified.
    static QueryPlanPtr
    addBatchJoin(QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan, NodeFunctionPtr onProbeKey, NodeFunctionPtr onBuildKey);

    /// Adds a
    static QueryPlanPtr addSink(std::string sinkName, QueryPlanPtr queryPlan, WorkerId workerId = INVALID_WORKER_NODE_ID);

    /// Create watermark assigner operator and adds it to the queryPlan
    static QueryPlanPtr
    assignWatermark(QueryPlanPtr queryPlan, const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor);

    /// Checks in case a window is contained in the query.
    /// If a watermark operator exists in the queryPlan and if not adds a watermark strategy to the queryPlan.
    static QueryPlanPtr checkAndAddWatermarkAssignment(QueryPlanPtr queryPlan, Windowing::WindowTypePtr windowType);

private:
    /**
     * @brief This method checks if an NodeFunction is instance Of NodeFunctionFieldAccess for Join and BatchJoin
     * @param function the function node to test
     * @param side points out from which side, i.e., left or right query plan, the NodeFunction is
     * @return nodeFunction as NodeFunctionFieldAccess
     */
    static std::shared_ptr<NodeFunctionFieldAccess> asNodeFunctionFieldAccess(const NodeFunctionPtr& function, std::string side);

    /**
    * @brief: This method adds a binary operator to the query plan and updates the consumed sources
    * @param operatorNode the binary operator to add
    * @param: leftQueryPlan the left query plan of the binary operation
    * @param: rightQueryPlan the right query plan of the binary operation
    * @return the updated queryPlan
    */
    static QueryPlanPtr addBinaryOperatorAndUpdateSource(OperatorPtr operatorNode, QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan);
};
}
