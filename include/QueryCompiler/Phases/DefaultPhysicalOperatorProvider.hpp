#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#include <QueryCompiler/Phases/PhysicalOperatorProvider.hpp>
namespace NES {
namespace QueryCompilation {

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
    void lowerWindowOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerWatermarkAssignmentOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerJoinOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    OperatorNodePtr getJoinBuildInputOperator(JoinLogicalOperatorNodePtr joinOperator, std::vector<OperatorNodePtr> children);
};

}
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
