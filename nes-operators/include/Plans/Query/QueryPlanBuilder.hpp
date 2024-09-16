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

#include <string>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES
{
/**
 * This class adds the logical operators to the queryPlan and handles further conditions and updates on the updated queryPlan and its nodes, e.g.,
 * update the consumed sources after a binary operator or adds window characteristics to the join operator.
 */
class QueryPlanBuilder
{
public:
    /**
     * @brief: Creates a query plan from a particular source. The source is identified by its name.
     * During query processing the underlying source descriptor is retrieved from the source catalog.
     * @param sourceName name of the source to query. This name has to be registered in the query catalog.
     * @return the updated queryPlan
     */
    static QueryPlanPtr createQueryPlan(std::string sourceName);

    /**
      * @brief this call projects out the attributes in the parameter list
      * @param functions list of attributes
      * @param queryPlan the queryPlan to add the projection node
      * @return the updated queryPlan
      */
    static QueryPlanPtr addProjection(const std::vector<FunctionNodePtr>& functions, QueryPlanPtr queryPlan);

    /**
     * @brief this call add the rename operator to the queryPlan, this operator renames the source
     * @param newSourceName source name
     * @param queryPlan the queryPlan to add the rename node
     * @return the updated queryPlan
     */
    static QueryPlanPtr addRename(std::string const& newSourceName, QueryPlanPtr queryPlan);

    /**
     * @brief: this call add the filter operator to the queryPlan, the operator filters records according to the predicate. An
     * exemplary usage would be: filter(Attribute("f1" < 10))
     * @param filterFunction as function node containing the predicate
     * @param queryPlanPtr the queryPlan the filter node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addFilter(FunctionNodePtr const& filterFunction, QueryPlanPtr queryPlan);

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
    static QueryPlanPtr addMap(FieldAssignmentFunctionNodePtr const& mapFunction, QueryPlanPtr queryPlan);

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
        FunctionNodePtr joinFunction,
        const Windowing::WindowTypePtr& windowType,
        Join::LogicalJoinDescriptor::JoinType joinType);

    /**
     * @brief This methods add the batch join operator to a query
     * @note In contrast to joinWith(), batchJoinWith() does not require a window to be specified.
     * @param leftQueryPlan the left query plan to combine by the join
     * @param rightQueryPlan the right query plan to combine by the join
     * @param onProbeKey key attribute of the left source
     * @param onBuildKey key attribute of the right source
     * @return the updated queryPlan
     */
    static QueryPlanPtr
    addBatchJoin(QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan, FunctionNodePtr onProbeKey, FunctionNodePtr onBuildKey);
    /**
     * @brief Adds the sink operator to the queryPlan.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor to add to the queryPlan
     * @param workerId id of the worker node where sink need to be placed
     * @return the updated queryPlan
     */
    static QueryPlanPtr addSink(QueryPlanPtr queryPlan, SinkDescriptorPtr sinkDescriptor, WorkerId workerId = INVALID_WORKER_NODE_ID);

    /**
     * @brief Create watermark assigner operator and adds it to the queryPlan
     * @param watermarkStrategyDescriptor which represents the semantic of this watermarkStrategy.
     * @return queryPlan
     */
    static QueryPlanPtr
    assignWatermark(QueryPlanPtr queryPlan, Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor);

    /**
    * @brief: Method that checks in case a window is contained in the query
    * if a watermark operator exists in the queryPlan and if not adds a watermark strategy to the queryPlan
    * @param: windowTypePtr the window description assigned to the query plan
    * @param queryPlan the queryPlan to check and add the watermark strategy to
    * @return the updated queryPlan
    */
    static QueryPlanPtr checkAndAddWatermarkAssignment(QueryPlanPtr queryPlan, const Windowing::WindowTypePtr windowType);

private:
    /**
     * @brief This method checks if an FunctionNode is instance Of FieldAccessFunctionNode for Join and BatchJoin
     * @param function the function node to test
     * @param side points out from which side, i.e., left or right query plan, the FunctionNode is
     * @return functionNode as FieldAccessFunctionNode
     */
    static std::shared_ptr<FieldAccessFunctionNode> checkFunction(FunctionNodePtr function, std::string side);

    /**
    * @brief: This method adds a binary operator to the query plan and updates the consumed sources
    * @param operatorNode the binary operator to add
    * @param: leftQueryPlan the left query plan of the binary operation
    * @param: rightQueryPlan the right query plan of the binary operation
    * @return the updated queryPlan
    */
    static QueryPlanPtr addBinaryOperatorAndUpdateSource(OperatorPtr operatorNode, QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan);
};
} /// end namespace NES
