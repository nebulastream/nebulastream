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

#ifndef NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_
#define NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_

#include <API/Query.hpp>
#include <API/Expressions/Expressions.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <string>

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
      * @param attribute list
      * @return the updated queryplanptr
      */
    static NES::QueryPlanPtr addProjectNode(std::vector<NES::ExpressionNodePtr> expressions, NES::QueryPlanPtr qplan);

    /**
     * Operator to rename the source
     * @param new source name
     * @return the query
     */
    static NES::QueryPlanPtr addRenameNode(std::string const& newSourceName, NES::QueryPlanPtr qplan);

    /**
     * @brief: Filter records according to the predicate. An
     * examplary usage would be: filter(Attribute("f1" < 10))
     * @param predicate as expression node
     * @param qplan the queryplan the map is added to
     * @return the updated queryplanptr
     */
    static NES::QueryPlanPtr addFilterNode(NES::ExpressionNodePtr const& filterExpression, NES::QueryPlanPtr qplan);

    /**
     * @brief: Map records according to a map expression. An
     * examplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
     * @param map expression
     * @param qplan the queryplan the map is added to
     * @return the updated queryplanptr
     */
    static NES::QueryPlanPtr addMapNode(NES::FieldAssignmentExpressionNodePtr const& mapExpression, NES::QueryPlanPtr qplan);

    /**
    * @brief: UnionOperator to combine two query plans
    * @param currentPlan the query plan to add the operator
    * @param: right is the right query plan
    * @return the updated queryplanptr
    */
    static NES::QueryPlanPtr addUnionOperatorNode(NES::QueryPlanPtr currentPlan, NES::QueryPlanPtr right);

   /**
     * We call it only internal as a last step during the Join/AND operation
     * @brief This methods add the join operator to a query
     * @param: currentPlan the query plan to add the operator
     * @param right subQuery to be joined
     * @param onLeftKey key attribute of the left source
     * @param onLeftKey key attribute of the right source
     * @param windowType Window definition.
     * @param joinType the definition of how the composition of the sources should be performed, i.e., INNER_JOIN or CARTESIAN_PRODUCT
     * @return the updated queryplanptr
     */
    static NES::QueryPlanPtr addJoinOperatorNode(NES::QueryPlanPtr currentPlan, NES::QueryPlanPtr right,
                                                       NES::ExpressionItem onLeftKey,
                                                       NES::ExpressionItem onRightKey,
                                                       const NES::Windowing::WindowTypePtr& windowType,
                                                       NES::Join::LogicalJoinDefinition::JoinType joinType);

    /**
     * @brief This methods add the batch join operator to a query
     * @note In contrast to joinWith(), batchJoinWith() does not require a window to be specified.
     * @param: currentPlan the query plan to add the operator
     * @param right subQuery to be joined
     * @param onProbeKey key attribute of the left stream
     * @param onBuildKey key attribute of the right stream
     * @return the query
     */
    static NES::QueryPlanPtr addBatchJoinOperatorNode(NES::QueryPlanPtr currentPlan, NES::QueryPlanPtr right, NES::ExpressionItem onProbeKey, NES::ExpressionItem onBuildKey);

    /**
    * @brief: checks and adds watermarks to stateful operator
    * @param: windowTypePtr the window assigned to the query plan
    * @param qplan the queryPlan to check and add watermarks to
    * @return the updated queryplanptr
    */
    static NES::QueryPlanPtr checkAndAddWatermarkAssignment(NES::QueryPlanPtr qplan,
                                                            const NES::Windowing::WindowTypePtr ptr);

    /**
    * @brief: This method adds a binary operator to the query plan and updates the consumed sources
    * @param: currentPlan the query plan to add the operator
    * @param: right is the right query plan
    * @return the updated queryplanptr
    */
    static NES::QueryPlanPtr addBinaryOperatorAndUpdateSource(NES::OperatorNodePtr op1,
                                                              NES::QueryPlanPtr currentPlan,
                                                              NES::QueryPlanPtr right);

    /**
     * @brief checks if an ExpressionNode is instance Of FieldAccessExpressionNode for Join and BatchJoin
     * @param expression the expression node to test
     * @param side (left or right side ExpressionNode)
     * @return expressionNode as FieldAccessExpressionNode
     */
    static std::shared_ptr<NES::FieldAccessExpressionNode> checkExpressionNode(NES::ExpressionNodePtr expression, std::string side);

    /**
     * @brief Add sink operator to the queryPlan.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    static NES::QueryPlanPtr addSinkeNode(NES::QueryPlanPtr currentPlan,NES::SinkDescriptorPtr sinkDescriptor);

    /**
     * @brief: Create watermark assigner operator and adds it to the queryPlan
     * @param watermarkStrategyDescriptor
     * @return queryPlan
     */
    static NES::QueryPlanPtr assignWatermarkNode(NES::QueryPlanPtr currentPlan, NES::Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor);
};

#endif//NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_
