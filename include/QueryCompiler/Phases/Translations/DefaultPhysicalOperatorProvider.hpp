/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <vector>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Provides a set of default lowerings for logical operators to corresponding physical operators.
 */
class DefaultPhysicalOperatorProvider : public PhysicalOperatorProvider {
  public:
    static PhysicalOperatorProviderPtr create();
    void lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) override;

  protected:
    /**
     * @brief Insets demultiplex operator before the current operator.
     * @param operatorNode
     */
    void insertDemultiplexOperatorsBefore(LogicalOperatorNodePtr operatorNode);
    /**
     * @brief Insert multiplex operator after the current operator.
     * @param operatorNode
     */
    void insertMultiplexOperatorsAfter(LogicalOperatorNodePtr operatorNode);
    /**
     * @brief Checks if the current operator is a demultiplexer, if it has multiple parents.
     * @param operatorNode
     * @return
     */
    bool isDemultiplex(LogicalOperatorNodePtr operatorNode);

    /**
     * @brief Lowers a binary operator
     * @param queryPlan current plan
     * @param operatorNode current operator
     */
    void lowerBinaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a unary operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUnaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a union operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUnionOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a project operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerProjectOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a map operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerMapOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a window operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerWindowOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a watermark assignment operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerWatermarkAssignmentOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a join operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerJoinOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);

    /**
    * @brief Lowers a join build operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    OperatorNodePtr
    getJoinBuildInputOperator(JoinLogicalOperatorNodePtr joinOperator, SchemaPtr schema, std::vector<OperatorNodePtr> children);
};

}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
