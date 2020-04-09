#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/LessThenExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/Operator.hpp>
namespace NES {

TranslateToLegacyPlanPhasePtr TranslateToLegacyPlanPhase::create() {
    return std::make_shared<TranslateToLegacyPlanPhase>();
}

OperatorPtr TranslateToLegacyPlanPhase::transformIndividualOperator(OperatorNodePtr operatorNode) {
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        auto sourceNodeOperator = operatorNode->as<SourceLogicalOperatorNode>();
        return createSourceOperator(sourceNodeOperator->getDataSource());

    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterNodeOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto legacyExpression = transformExpression(filterNodeOperator->getPredicate());
        // we can assume that the legacy expression is always a predicate.
        auto legacyPredicate = std::const_pointer_cast<Predicate>(legacyExpression);
        if (legacyPredicate == nullptr) {
            NES_FATAL_ERROR("TranslateToLegacyPhase: Error during translating expression");
        }
        return createFilterOperator(legacyPredicate);

    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto sinkNodeOperator = operatorNode->as<SinkLogicalOperatorNode>();
        return createSinkOperator(sinkNodeOperator->getDataSink());
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this operator node: " << operatorNode);
    NES_NOT_IMPLEMENTED;
}

OperatorPtr TranslateToLegacyPlanPhase::transform(OperatorNodePtr operatorNode) {
    auto legacyOperator = transformIndividualOperator(operatorNode);
    for (const NodePtr& child: operatorNode->getChildren()) {
        auto legacyChildOperator = transform(child->as<OperatorNode>());
        legacyOperator->addChild(legacyChildOperator);
    }
    return legacyOperator;
}

UserAPIExpressionPtr TranslateToLegacyPlanPhase::transformExpression(ExpressionNodePtr expression) {
    if (expression->instanceOf<ConstantValueExpressionNode>()) {
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        return PredicateItem(value).copy();
    } else if (expression->instanceOf<FieldReadExpressionNode>()) {
        auto fieldReadExpression = expression->as<FieldReadExpressionNode>();
        auto fieldName = fieldReadExpression->getFieldName();
        auto value = fieldReadExpression->getStamp();
        return Field(AttributeField(fieldName, value).copy()).copy();
    } else if (expression->instanceOf<AndExpressionNode>()) {
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_AND_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessThenExpressionNode>()) {
        auto andExpressionNode = expression->as<LessThenExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THEN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        auto andExpressionNode = expression->as<EqualsExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::EQUAL_OP, legacyLeft, legacyRight).copy();
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this expression: " << expression);
    NES_NOT_IMPLEMENTED;
}

}