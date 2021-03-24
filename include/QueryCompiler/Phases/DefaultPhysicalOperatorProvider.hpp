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
#include <QueryCompiler/Phases/PhysicalOperatorProvider.hpp>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Provides a set of default lowerings for logical operators to corresponding physical operators.
 */
class DefaultPhysicalOperatorProvider : public PhysicalOperatorProvider{
  public:
    static PhysicalOperatorProviderPtr create();
    void lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) override;

  protected:
    void insertDemulticastOperatorsBefore(LogicalOperatorNodePtr operatorNode);
    void insertMulticastOperatorsAfter(LogicalOperatorNodePtr operatorNode);
    bool isDemulticast(LogicalOperatorNodePtr operatorNode);

    void lowerBinaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerUnaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerUnionOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerProjectOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerMapOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerWindowOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerWatermarkAssignmentOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerJoinOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    OperatorNodePtr getJoinBuildInputOperator(JoinLogicalOperatorNodePtr joinOperator, std::vector<OperatorNodePtr> children);
};

}
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
