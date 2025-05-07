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
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <LogicalOperators/Windows/JoinOperator.hpp>
#include <LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <LogicalPlans/Plan.hpp>

namespace NES::Logical
{
/// This class adds the logical operators to the queryPlan and handles further conditions and updates on the updated queryPlan and its nodes, e.g.,
/// update the consumed sources after a binary operator or adds window characteristics to the join operator.
class PlanBuilder
{
public:
    /// Creates a query plan from a particular source. The source is identified by its name.
    /// During query processing the underlying source descriptor is retrieved from the source catalog.
    static Plan createPlan(std::string logicalSourceName);

    /// @brief this call projects out the attributes in the parameter list
    /// @param functions list of attributes
    /// @param queryPlan the queryPlan to add the projection node
    /// @return the updated queryPlan
    static Plan addProjection(std::vector<Function> functions, Plan queryPlan);

    /// @brief: this call add the selection operator to the queryPlan, the operator selections records according to the predicate.
    /// @param selectionFunction as function node containing the predicate
    /// @param Plan the queryPlan the selection node is added to
    /// @return the updated queryPlan
    static Plan addSelection(Function selectionFunction, Plan queryPlan);

    /// @brief: Map records according to a map function.
    /// @param mapFunction as function node
    /// @param queryPlan the queryPlan the map is added to
    /// @return the updated Plan
    static Plan addMap(Function mapFunction, Plan queryPlan);

    static Plan addWindowAggregation(
        Plan queryPlan,
        std::shared_ptr<Windowing::WindowType> windowType,
        std::vector<std::shared_ptr<WindowAggregationFunction>> windowAggs,
        std::vector<FieldAccessFunction> onKeys);

    /// @brief UnionOperator to combine two query plans
    /// @param leftPlan the left query plan to combine by the union
    /// @param rightPlan the right query plan to combine by the union
    /// @return the updated queryPlan combining left and rightPlan with union
    static Plan addUnion(Plan leftPlan, Plan rightPlan);

    /// @brief This methods add the join operator to a query
    /// @param leftPlan the left query plan to combine by the join
    /// @param rightPlan the right query plan to combine by the join
    /// @param joinFunction set of join Functions
    /// @param windowType Window definition.
    /// @return the updated queryPlan
    static Plan addJoin(
        Plan leftPlan,
        Plan rightPlan,
        Function joinFunction,
        std::shared_ptr<Windowing::WindowType> windowType,
        JoinOperator::JoinType joinType);

    static Plan addSink(std::string sinkName, Plan queryPlan, WorkerId workerId = INVALID_WORKER_NODE_ID);

    /// Checks in case a window is contained in the query.
    /// If a watermark operator exists in the queryPlan and if not adds a watermark strategy to the queryPlan.
    static Plan checkAndAddWatermarkAssigner(Plan queryPlan, std::shared_ptr<Windowing::WindowType> windowType);

private:
    /// @brief: This method adds a binary operator to the query plan and updates the consumed sources
    /// @param operatorNode the binary operator to add
    /// @param: leftPlan the left query plan of the binary operation
    /// @param: rightPlan the right query plan of the binary operation
    /// @return the updated queryPlan
    static Plan
    addBinaryOperatorAndUpdateSource(Operator operatorNode, Plan leftPlan, Plan rightPlan);
};
}
