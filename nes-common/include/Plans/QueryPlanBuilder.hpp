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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Plans/QueryPlan.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>

namespace NES
{
/// This class adds the logical operators to the queryPlan and handles further conditions and updates on the updated queryPlan and its nodes, e.g.,
/// update the consumed sources after a binary operator or adds window characteristics to the join operator.
class QueryPlanBuilder
{
public:
    /// Creates a query plan from a particular source. The source is identified by its name.
    /// During query processing the underlying source descriptor is retrieved from the source catalog.
    static QueryPlan createQueryPlan(std::string logicalSourceName);

    /// @brief this call projects out the attributes in the parameter list
    /// @param functions list of attributes
    /// @param queryPlan the queryPlan to add the projection node
    /// @return the updated queryPlan
    static QueryPlan
    addProjection(std::vector<std::unique_ptr<LogicalFunction>> functions, QueryPlan queryPlan);

    /// @brief this call add the rename operator to the queryPlan, this operator renames the source
    /// @param newSourceName source name
    /// @param queryPlan the queryPlan to add the rename node
    /// @return the updated queryPlan
    static QueryPlan addRename(const std::string& newSourceName, QueryPlan queryPlan);

    /// @brief: this call add the selection operator to the queryPlan, the operator selections records according to the predicate. An
    /// exemplary usage would be: selection(Attribute("f1" < 10))
    /// @param selectionFunction as function node containing the predicate
    /// @param std::shared_ptr<QueryPlan> the queryPlan the selection node is added to
    /// @return the updated queryPlan
    static QueryPlan
    addSelection(std::unique_ptr<LogicalFunction> selectionFunction, QueryPlan queryPlan);

    /// @brief: Map records according to a map function. An
    /// exemplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
    /// @param mapFunction as function node
    /// @param queryPlan the queryPlan the map is added to
    /// @return the updated std::shared_ptr<QueryPlan>
    static QueryPlan
    addMap(std::unique_ptr<FieldAssignmentLogicalFunction> mapFunction, QueryPlan queryPlan);

    static QueryPlan addWindowAggregation(
        QueryPlan queryPlan,
        std::unique_ptr<Windowing::WindowType> windowType,
        std::vector<std::unique_ptr<Windowing::WindowAggregationFunction>> windowAggs,
        std::vector<std::unique_ptr<FieldAccessLogicalFunction>> onKeys);

    /// @brief UnionOperator to combine two query plans
    /// @param leftQueryPlan the left query plan to combine by the union
    /// @param rightQueryPlan the right query plan to combine by the union
    /// @return the updated queryPlan combining left and rightQueryPlan with union
    static QueryPlan addUnion(QueryPlan leftQueryPlan, QueryPlan rightQueryPlan);

    /// @brief This methods add the join operator to a query
    /// @param leftQueryPlan the left query plan to combine by the join
    /// @param rightQueryPlan the right query plan to combine by the join
    /// @param joinFunction set of join Functions
    /// @param windowType Window definition.
    /// @return the updated queryPlan
    static QueryPlan addJoin(
        QueryPlan leftQueryPlan,
        QueryPlan rightQueryPlan,
        std::unique_ptr<LogicalFunction> joinFunction,
        std::unique_ptr<Windowing::WindowType> windowType,
        JoinLogicalOperator::JoinType joinType);

    static QueryPlan addSink(std::string sinkName, QueryPlan queryPlan, WorkerId workerId = INVALID_WORKER_NODE_ID);

    // TODO
    /// Create watermark assigner operator and adds it to the queryPlan
    //static std::shared_ptr<QueryPlan> assignWatermark(
    //    std::shared_ptr<QueryPlan> queryPlan, const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor);

    /// Checks in case a window is contained in the query.
    /// If a watermark operator exists in the queryPlan and if not adds a watermark strategy to the queryPlan.
    static std::shared_ptr<QueryPlan>
    checkAndAddWatermarkAssignment(std::shared_ptr<QueryPlan> queryPlan, std::shared_ptr<Windowing::WindowType> windowType);

private:
    /// @brief This method checks if an LogicalFunction is instance Of FieldAccessLogicalFunction for Join and BatchJoin
    /// @param function the function node to test
    /// @param side points out from which side, i.e., left or right query plan, the LogicalFunction is
    // @return LogicalFunction as FieldAccessLogicalFunction
    static std::unique_ptr<FieldAccessLogicalFunction>
    asFieldAccessLogicalFunction(std::unique_ptr<LogicalFunction> function, std::string side);

    /// @brief: This method adds a binary operator to the query plan and updates the consumed sources
    /// @param operatorNode the binary operator to add
    /// @param: leftQueryPlan the left query plan of the binary operation
    /// @param: rightQueryPlan the right query plan of the binary operation
    /// @return the updated queryPlan
    static QueryPlan addBinaryOperatorAndUpdateSource(
        std::unique_ptr<Operator> operatorNode, QueryPlan leftQueryPlan, QueryPlan rightQueryPlan);
};
}
