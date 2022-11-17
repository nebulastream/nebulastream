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

#include <API/Expressions/Expressions.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <string>

/**
 * This class adds the Logical Operators to the Query Plan and handles further conditions ands updates on the query plan and its nodes, e.g.,
 * update the consumed sources after a binary operator or adds window characteristics to the join operator.
 * TODO: This class is part of a refactoring of the APIs and is right now a duplicate of the Query class excluding the C++ contents
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
    * @brief: JoinOperator to combine two query plans
    * @param: currentPlan the query plan to add the operator
    * @param: right is the right query plan
    * @return the updated queryplanptr
    */
    static NES::QueryPlanPtr addJoinOperatorNode(NES::QueryPlanPtr currentPlan, NES::QueryPlanPtr right,
                                                       NES::ExpressionItem onLeftKey,
                                                       NES::ExpressionItem onRightKey,
                                                       const NES::Windowing::WindowTypePtr& windowType,
                                                       NES::Join::LogicalJoinDefinition::JoinType joinType);
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
};

#endif//NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_
