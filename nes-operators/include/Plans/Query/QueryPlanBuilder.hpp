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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/Operator.hpp>
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
    static std::shared_ptr<QueryPlan> createQueryPlan(IdentifierList logicalSourceName);

    /**
      * @brief this call projects out the attributes in the parameter list
      * @param functions list of attributes
      * @param queryPlan the queryPlan to add the projection node
      * @return the updated queryPlan
      */
    static std::shared_ptr<QueryPlan>
    addProjection(const std::vector<std::shared_ptr<NodeFunction>>& functions, std::shared_ptr<QueryPlan> queryPlan);

    /**
     * @brief this call add the rename operator to the queryPlan, this operator renames the source
     * @param newSourceName source name
     * @param queryPlan the queryPlan to add the rename node
     * @return the updated queryPlan
     */
    static std::shared_ptr<QueryPlan> addRename(const IdentifierList& newSourceName, std::shared_ptr<QueryPlan> queryPlan);

    /**
     * @brief: this call add the selection operator to the queryPlan, the operator selections records according to the predicate. An
     * exemplary usage would be: selection(Attribute("f1" < 10))
     * @param selectionFunction as function node containing the predicate
     * @param std::shared_ptr<QueryPlan> the queryPlan the selection node is added to
     * @return the updated queryPlan
     */
    static std::shared_ptr<QueryPlan>
    addSelection(const std::shared_ptr<NodeFunction>& selectionFunction, std::shared_ptr<QueryPlan> queryPlan);

    /**
     * @brief: this call adds the limit operator to the queryPlan, the operator limits the number of produced records.
     * @param filterFunction as function node containing the predicate
     * @param std::shared_ptr<QueryPlan> the queryPlan the filter node is added to
     * @return the updated queryPlan
     */
    static std::shared_ptr<QueryPlan> addLimit(uint64_t limit, std::shared_ptr<QueryPlan> queryPlan);

    /**
     * @brief: Map records according to a map function. An
     * exemplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
     * @param mapFunction as function node
     * @param queryPlan the queryPlan the map is added to
     * @return the updated std::shared_ptr<QueryPlan>
     */
    static std::shared_ptr<QueryPlan>
    addMap(const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction, std::shared_ptr<QueryPlan> queryPlan);

    static std::shared_ptr<QueryPlan> addWindowAggregation(
        std::shared_ptr<QueryPlan> queryPlan,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        const std::vector<std::shared_ptr<Windowing::WindowAggregationDescriptor>>& windowAggs,
        const std::vector<std::shared_ptr<NodeFunctionFieldAccess>>& onKeys);

    /**
    * @brief UnionOperator to combine two query plans
    * @param leftQueryPlan the left query plan to combine by the union
    * @param rightQueryPlan the right query plan to combine by the union
    * @return the updated queryPlan combining left and rightQueryPlan with union
    */
    static std::shared_ptr<QueryPlan> addUnion(std::shared_ptr<QueryPlan> leftQueryPlan, const std::shared_ptr<QueryPlan>& rightQueryPlan);

    /**
     * @brief This methods add the join operator to a query
     * @param leftQueryPlan the left query plan to combine by the join
     * @param rightQueryPlan the right query plan to combine by the join
     * @param joinFunction set of join Functions
     * @param windowType Window definition.
     * @return the updated queryPlan
     */
    static std::shared_ptr<QueryPlan> addJoin(
        std::shared_ptr<QueryPlan> leftQueryPlan,
        std::shared_ptr<QueryPlan> rightQueryPlan,
        const std::shared_ptr<NodeFunction>& joinFunction,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        Join::LogicalJoinDescriptor::JoinType joinType);

    /// Adds a sink operator to the query plan
    static std::shared_ptr<QueryPlan> addSink(IdentifierList sinkName, std::shared_ptr<QueryPlan> queryPlan);
    static std::shared_ptr<QueryPlan> addSink(IdentifierList sinkName, std::shared_ptr<QueryPlan> queryPlan, WorkerId workerId);

    /// Create watermark assigner operator and adds it to the queryPlan
    static std::shared_ptr<QueryPlan> assignWatermark(
        std::shared_ptr<QueryPlan> queryPlan, const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor);

    /// Checks in case a window is contained in the query.
    /// If a watermark operator exists in the queryPlan and if not adds a watermark strategy to the queryPlan.
    static std::shared_ptr<QueryPlan>
    checkAndAddWatermarkAssignment(std::shared_ptr<QueryPlan> queryPlan, const std::shared_ptr<Windowing::WindowType>& windowType);

private:
    /**
     * @brief This method checks if an NodeFunction is instance Of NodeFunctionFieldAccess for Joins
     * @param function the function node to test
     * @param side points out from which side, i.e., left or right query plan, the NodeFunction is
     * @return nodeFunction as NodeFunctionFieldAccess
     */
    static std::shared_ptr<NodeFunctionFieldAccess>
    asNodeFunctionFieldAccess(const std::shared_ptr<NodeFunction>& function, std::string side);

    /**
    * @brief: This method adds a binary operator to the query plan and updates the consumed sources
    * @param operatorNode the binary operator to add
    * @param: leftQueryPlan the left query plan of the binary operation
    * @param: rightQueryPlan the right query plan of the binary operation
    * @return the updated queryPlan
    */
    static std::shared_ptr<QueryPlan> addBinaryOperatorAndUpdateSource(
        const std::shared_ptr<Operator>& operatorNode,
        std::shared_ptr<QueryPlan> leftQueryPlan,
        const std::shared_ptr<QueryPlan>& rightQueryPlan);
};
}
