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
    void ingestDemulticastOperatorsBefore(LogicalOperatorNodePtr operatorNode);
    void ingestMulticastOperatorsAfter(LogicalOperatorNodePtr operatorNode);
    bool isDemulticast(LogicalOperatorNodePtr operatorNode);
    void lowerBinaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerUnaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerUnionOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
    void lowerJoinOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
};

}
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
