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
#include <unordered_map>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include "DataTypes/UnboundSchema.hpp"
#include "Functions/UnboundFieldAccessLogicalFunction.hpp"
#include "Operators/Windows/WindowedAggregationLogicalOperator.hpp"

namespace NES
{
/// This class adds the logical operators to the queryPlan and handles further conditions and updates on the updated queryPlan and its nodes, e.g.,
/// update the consumed sources after a binary operator or adds window characteristics to the join operator.
class LogicalPlanBuilder
{
public:
    /// Creates a query plan from a particular source. The source is identified by its name.
    /// During query processing the underlying source descriptor is retrieved from the source catalog.
    static LogicalPlan createLogicalPlan(Identifier logicalSourceName);

    static LogicalPlan createLogicalPlan(
        Identifier inlineSourceType,
        SchemaBase<UnboundFieldBase<1>, true> schema,
        std::unordered_map<Identifier, std::string> sourceConfig,
        std::unordered_map<Identifier, std::string> parserConfig);

    /// @brief this call projects out the attributes in the parameter list
    /// @param functions list of attributes
    /// @param asterisk project everything in addition to the projections
    /// @param queryPlan the queryPlan to add the projection node
    /// @return the updated queryPlan
    static LogicalPlan
    addProjection(std::vector<ProjectionLogicalOperator::UnboundProjection> projections, bool asterisk, const LogicalPlan& queryPlan);

    /// @brief: this call adds the selection operator to the queryPlan; the operator selects records according to the predicate.
    /// @param selectionFunction a function node containing the predicate
    /// @param LogicalPlan the queryPlan the selection node is added to
    /// @return the updated queryPlan
    static LogicalPlan addSelection(LogicalFunction selectionFunction, const LogicalPlan& queryPlan);

    static LogicalPlan addWindowAggregation(
        LogicalPlan queryPlan,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> windowAggs,
        std::vector<UnboundFieldAccessLogicalFunction> onKeys,
        Windowing::TimeCharacteristic timeCharacteristic);

    /// @brief UnionOperator to combine two query plans
    /// @param leftLogicalPlan the left query plan to combine by the union
    /// @param rightLogicalPlan the right query plan to combine by the union
    /// @return the updated queryPlan combining left and rightLogicalPlan with union
    static LogicalPlan addUnion(LogicalPlan leftLogicalPlan, LogicalPlan rightLogicalPlan);

    /// @brief This methods add the join operator to a query
    /// @param leftLogicalPlan the left query plan to combine by the join
    /// @param rightLogicalPlan the right query plan to combine by the join
    /// @param joinFunction set of join Functions
    /// @param windowType Window definition.
    /// @return the updated queryPlan
    static LogicalPlan addJoin(
        LogicalPlan leftLogicalPlan,
        LogicalPlan rightLogicalPlan,
        const LogicalFunction& joinFunction,
        std::shared_ptr<Windowing::WindowType> windowType,
        JoinLogicalOperator::JoinType joinType,
        Windowing::TimeCharacteristic leftCharacteristic,
        Windowing::TimeCharacteristic rightCharacteristic);

    static LogicalPlan addSink(Identifier sinkName, const LogicalPlan& queryPlan);
    static LogicalPlan addInlineSink(
        Identifier type, SchemaBase<UnboundFieldBase<1>, true> schema, std::unordered_map<Identifier, std::string> sinkConfig, const LogicalPlan& queryPlan);

    /// Checks in case a window is contained in the query.
    /// If a watermark operator exists in the queryPlan and if not adds a watermark strategy to the queryPlan.
    static LogicalPlan checkAndAddWatermarkAssigner(LogicalPlan queryPlan, const Windowing::TimeCharacteristic& timeCharacteristic);

private:
    /// @brief: This method adds a binary operator to the query plan and updates the consumed sources
    /// @param operatorNode the binary operator to add
    /// @param: leftLogicalPlan the left query plan of the binary operation
    /// @param: rightLogicalPlan the right query plan of the binary operation
    /// @return the updated queryPlan
    static LogicalPlan addBinaryOperatorAndUpdateSource(
        const LogicalOperator& operatorNode, const LogicalPlan& leftLogicalPlan, const LogicalPlan& rightLogicalPlan);
};
}
