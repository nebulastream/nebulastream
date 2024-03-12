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

#ifndef NES_OPERATORS_INCLUDE_PLANS_QUERY_QUERYPLANBUILDER_HPP_
#define NES_OPERATORS_INCLUDE_PLANS_QUERY_QUERYPLANBUILDER_HPP_

#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <string>

namespace NES {
/**
 * This class adds the logical operators to the queryPlan and handles further conditions and updates on the updated queryPlan and its nodes, e.g.,
 * update the consumed sources after a binary operator or adds window characteristics to the join operator.
 */
class QueryPlanBuilder {
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
      * @param expressions list of attributes
      * @param queryPlan the queryPlan to add the projection node
      * @return the updated queryPlan
      */
    static QueryPlanPtr addProjection(std::vector<ExpressionNodePtr> expressions, QueryPlanPtr queryPlan);

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
     * @param filterExpression as expression node containing the predicate
     * @param queryPlanPtr the queryPlan the filter node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addFilter(ExpressionNodePtr const& filterExpression, QueryPlanPtr queryPlan);

    /**
     * @brief: this call adds the limit operator to the queryPlan, the operator limits the number of produced records.
     * @param filterExpression as expression node containing the predicate
     * @param queryPlanPtr the queryPlan the filter node is added to
     * @return the updated queryPlan
     */
    static QueryPlanPtr addLimit(const uint64_t limit, QueryPlanPtr queryPlan);

    /**
     * @brief: Map records according to a map expression. An
     * exemplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
     * @param mapExpression as expression node
     * @param queryPlan the queryPlan the map is added to
     * @return the updated queryPlanPtr
     */
    static QueryPlanPtr addMap(FieldAssignmentExpressionNodePtr const& mapExpression, QueryPlanPtr queryPlan);

    /**
     * @brief Adds a synopsis build operator to this query
     * @param window
     * @param statisticDescriptor: Descriptor for the synopsis
     * @param metricHash: The hash of the metric, this operator is collecting, e.g., `cardinality` over field `f1`
     * @param queryPlan the queryPlan the synopsis is added to
     * @return the updated queryPlanPtr
     */
    static QueryPlanPtr addStatisticBuildOperator(Windowing::WindowTypePtr window,
                                                  Statistic::WindowStatisticDescriptorPtr statisticDescriptor,
                                                  Statistic::MetricHash metricHash,
                                                  QueryPlanPtr queryPlan);

    /**
     * @brief: Map java udf according to the java method given in the descriptor.
     * @param descriptor as java udf descriptor
     * @param queryPlan the queryPlan the map is added to
     * @return the updated queryPlanPtr
     */
    static QueryPlanPtr addMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor, QueryPlanPtr queryPlan);

    /**
     * @brief: FlatMap java udf according to the java method given in the descriptor.
     * @param descriptor as java udf descriptor
     * @param queryPlan the queryPlan the map is added to
     * @return the updated queryPlanPtr
     */
    static QueryPlanPtr addFlatMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor, QueryPlanPtr queryPlan);

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
     * @param onLeftKey key attribute of the left source
     * @param onRightKey key attribute of the right source
     * @param windowType Window definition.
     * @param joinType the definition of how the composition of the sources should be performed, i.e., INNER_JOIN or CARTESIAN_PRODUCT
     * @return the updated queryPlan
     */
    static QueryPlanPtr addJoin(QueryPlanPtr leftQueryPlan,
                                QueryPlanPtr rightQueryPlan,
                                ExpressionNodePtr onLeftKey,
                                ExpressionNodePtr onRightKey,
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
    static QueryPlanPtr addBatchJoin(QueryPlanPtr leftQueryPlan,
                                     QueryPlanPtr rightQueryPlan,
                                     ExpressionNodePtr onProbeKey,
                                     ExpressionNodePtr onBuildKey);
    /**
     * @brief Adds the sink operator to the queryPlan.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor to add to the queryPlan
     * @param workerId id of the worker node where sink need to be placed
     * @return the updated queryPlan
     */
    static QueryPlanPtr
    addSink(QueryPlanPtr queryPlan, SinkDescriptorPtr sinkDescriptor, WorkerId workerId = INVALID_WORKER_NODE_ID);

    /**
     * @brief Create watermark assigner operator and adds it to the queryPlan
     * @param watermarkStrategyDescriptor which represents the semantic of this watermarkStrategy.
     * @return queryPlan
     */
    static QueryPlanPtr assignWatermark(QueryPlanPtr queryPlan,
                                        Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor);

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
     * @brief This method checks if an ExpressionNode is instance Of FieldAccessExpressionNode for Join and BatchJoin
     * @param expression the expression node to test
     * @param side points out from which side, i.e., left or right query plan, the ExpressionNode is
     * @return expressionNode as FieldAccessExpressionNode
     */
    static std::shared_ptr<FieldAccessExpressionNode> checkExpression(ExpressionNodePtr expression, std::string side);

    /**
    * @brief: This method adds a binary operator to the query plan and updates the consumed sources
    * @param operatorNode the binary operator to add
    * @param: leftQueryPlan the left query plan of the binary operation
    * @param: rightQueryPlan the right query plan of the binary operation
    * @return the updated queryPlan
    */
    static QueryPlanPtr
    addBinaryOperatorAndUpdateSource(OperatorPtr operatorNode, QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan);
};
}// end namespace NES
#endif// NES_OPERATORS_INCLUDE_PLANS_QUERY_QUERYPLANBUILDER_HPP_
