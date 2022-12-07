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

#ifndef NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_HPP_
#define NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_HPP_

#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <string>

namespace NES {
/**
 * This class adds the Logical Operators to the Query Plan and handles further conditions ands updates on the query plan and its nodes, e.g.,
 * update the consumed sources after a binary operator or adds window characteristics to the join operator.
 */
class QueryPlanBuilder {
  public:
    /**
     * @brief: Creates a query plan from a particular source. The source is identified by its name.
     * During query processing the underlying source descriptor is retrieved from the source catalog.
     * @param sourceName name of the source to query. This name has to be registered in the query catalog.
     * @return the updated queryplanptr
     */
    static NES::QueryPlanPtr createQueryPlan(std::string sourceName);

    /**
      * @brief this call projects out the attributes in the parameter list
      * @param expressionPtr list of expressions
      * @param queryPlanPtr the queryPlan to add the projection node
      * @return the updated queryPlanPtr
      */
    static NES::QueryPlanPtr addProject(std::vector<NES::ExpressionNodePtr> expressionPtr, NES::QueryPlanPtr queryPlanPtr);

    /**
     * Operator to rename the source
     * @param newSourceName source name
     * @return the queryPlanPtr
     */
    static NES::QueryPlanPtr addRename(std::string const& newSourceName, NES::QueryPlanPtr queryPlanPtr);

    /**
     * @brief: Filter records according to the predicate. An
     * examplary usage would be: filter(Attribute("f1" < 10))
     * @param filterExpression as expression node
     * @param queryPlanPtr the queryPlan the filter node is added to
     * @return the updated queryPlanPtr
     */
    static NES::QueryPlanPtr addFilter(NES::ExpressionNodePtr const& filterExpression, NES::QueryPlanPtr queryPlanPtr);

    /**
     * @brief: Map records according to a map expression. An
     * examplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
     * @param mapExpression as expression node
     * @param queryPlan the queryPlan the map is added to
     * @return the updated queryPlanPtr
     */
    static NES::QueryPlanPtr addMap(NES::FieldAssignmentExpressionNodePtr const& mapExpression, NES::QueryPlanPtr queryPlanPtr);

    /**
    * @brief: UnionOperator to combine two query plans
    * @param queryPlanPtr the query plan to add the operator
    * @param: rightQueryPlanPtr is the rightQueryPlan query plan
    * @return the updated queryPlanPtr
    */
    static NES::QueryPlanPtr addUnionOperator(NES::QueryPlanPtr queryPlanPtr, NES::QueryPlanPtr rightQueryPlanPtr);

    /**
     * We call it only internal as a last step during the Join/AND operation
     * @brief This methods add the join operator to a query
     * @param: queryPlanPtr the query plan to add the operator
     * @param rightQueryPlanPtr subQuery to be joined
     * @param onLeftKey key attribute of the left source
     * @param onLeftKey key attribute of the rightQueryPlan source
     * @param windowType Window definition.
     * @param joinType the definition of how the composition of the sources should be performed, i.e., INNER_JOIN or CARTESIAN_PRODUCT
     * @return the updated queryplanptr
     */
    static NES::QueryPlanPtr addJoinOperator(NES::QueryPlanPtr queryPlanPtr,
                                             NES::QueryPlanPtr rightQueryPlanPtr,
                                             NES::ExpressionItem onLeftKey,
                                             NES::ExpressionItem onRightQueryPlanKey,
                                             const NES::Windowing::WindowTypePtr& windowType,
                                             NES::Join::LogicalJoinDefinition::JoinType joinType);

    /**
     * @brief This methods add the batch join operator to a query
     * @note In contrast to joinWith(), batchJoinWith() does not require a window to be specified.
     * @param: queryPlan the query plan to add the operator
     * @param rightQueryPlan subQuery to be joined
     * @param onProbeKey key attribute of the left stream
     * @param onBuildKey key attribute of the rightQueryPlan stream
     * @return the query
     */
    static NES::QueryPlanPtr addBatchJoinOperator(NES::QueryPlanPtr queryPlanPtr,
                                                  NES::QueryPlanPtr rightQueryPlanPtr,
                                                  NES::ExpressionItem onProbeKey,
                                                  NES::ExpressionItem onBuildKey);

    /**
    * @brief: checks if a watermark operator exists in the queryPlan if not adds watermarks to stateful operator
    * @param: windowTypePtr the window assigned to the query plan
    * @param queryPlan the queryPlan to check and add watermarks to
    * @return the updated queryplanptr
    */
    static NES::QueryPlanPtr checkAndAddWatermarkAssignment(NES::QueryPlanPtr queryPlanPtr, const NES::Windowing::WindowTypePtr ptr);

    /**
    * @brief: This method adds a binary operator to the query plan and updates the consumed sources
    * @param operatorNodePtr the binary operator to add
    * @param: queryPlan the query plan to add the operator
    * @param: rightQueryPlan is the right query plan to add
    * @return the updated queryplanptr
    */
    static NES::QueryPlanPtr
    addBinaryOperatorAndUpdateSource(NES::OperatorNodePtr operatorNodePtr, NES::QueryPlanPtr queryPlanPtr, NES::QueryPlanPtr rightQueryPlanPtr);

    /**
     * @brief checks if an ExpressionNode is instance Of FieldAccessExpressionNode for Join and BatchJoin
     * @param expression the expression node to test
     * @param side (left or rightQueryPlan side ExpressionNode)
     * @return expressionNode as FieldAccessExpressionNode
     */
    static std::shared_ptr<NES::FieldAccessExpressionNode> checkExpression(NES::ExpressionNodePtr expressionPtr, std::string side);

    /**
     * @brief Add sink operator to the queryPlan.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    static NES::QueryPlanPtr addSink(NES::QueryPlanPtr queryPlanPtr, NES::SinkDescriptorPtr sinkDescriptorPtr);

    /**
     * @brief: Create watermark assigner operator and adds it to the queryPlan
     * @param watermarkStrategyDescriptor
     * @return queryPlan
     */
    static NES::QueryPlanPtr assignWatermark(NES::QueryPlanPtr queryPlanPtr,
                                             NES::Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor);
};
} // end namespace NES
#endif//NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_
