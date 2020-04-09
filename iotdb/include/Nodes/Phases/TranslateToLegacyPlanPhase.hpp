#ifndef NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_
#define NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_

#include <memory>
#include <QueryCompiler/QueryCompiler.hpp>

namespace NES {

class TranslateToLegacyPlanPhase;
typedef std::shared_ptr<TranslateToLegacyPlanPhase> TranslateToLegacyPlanPhasePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;


class TranslateToLegacyPlanPhase {
  public:
    static TranslateToLegacyPlanPhasePtr create();
    OperatorPtr transform(OperatorNodePtr operatorNode);
  private:
    OperatorPtr transformIndividualOperator(OperatorNodePtr operatorNode);
    UserAPIExpressionPtr transformExpression(ExpressionNodePtr node);
};

}

#endif //NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_
