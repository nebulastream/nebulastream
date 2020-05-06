#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>

#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSource.hpp>
#include <Operators/Operator.hpp>
#include <API/UserAPIExpression.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Operators/Impl/SourceOperator.hpp>

namespace NES {

TranslateFromLegacyPlanPhasePtr TranslateFromLegacyPlanPhase::create() {
    return std::make_shared<TranslateFromLegacyPlanPhase>();
}

OperatorNodePtr TranslateFromLegacyPlanPhase::transformIndividualOperator(OperatorPtr operatorPtr) {

    if (operatorPtr->getOperatorType() == SOURCE_OP) {
        // Translate Source operator node.
        auto sourceOperator = std::dynamic_pointer_cast<SourceOperator>(operatorPtr);
        const DataSourcePtr dataSource = sourceOperator->getDataSourcePtr();
        return ConvertPhysicalToLogicalSource::createDataSource(dataSource);
    } else if (operatorPtr->getOperatorType() == FILTER_OP) {
        // Translate filter operator node.
        auto filterNodeOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto legacyExpression = transformExpression(filterNodeOperator->getPredicate());
        // we can assume that the legacy expression is always a predicate.
        auto legacyPredicate = std::dynamic_pointer_cast<Predicate>(legacyExpression);
        if (legacyPredicate == nullptr) {
            NES_FATAL_ERROR("TranslateToLegacyPhase: Error during translating filter expression");
        }
        return createFilterOperator(legacyPredicate);
    } else if (operatorPtr->getOperatorType() == MAP_OP) {
        // Translate map operator node.
        auto mapOperatorNode = operatorNode->as<MapLogicalOperatorNode>();
        auto mapExpression = mapOperatorNode->getMapExpression();
        // Translate to the legacy representation
        auto legacyFieldAccess = transformExpression(mapExpression->getField());
        auto legacyAssignment = transformExpression(mapExpression->getAssignment());
        // Cast to the proper type
        auto legacyPredicate = std::dynamic_pointer_cast<Predicate>(legacyAssignment);
        auto legacyField = std::dynamic_pointer_cast<Field>(legacyAssignment);
        if (legacyPredicate == nullptr || legacyField == nullptr) {
            NES_FATAL_ERROR("TranslateToLegacyPhase: Error during translating map expression");
        }
        // Create legacy map operator
        return createMapOperator(legacyField->getAttributeField(), legacyPredicate);

    } else if (operatorPtr->getOperatorType() == SINK_OP) {
        // Translate sink operator node.
        auto sinkNodeOperator = operatorNode->as<SinkLogicalOperatorNode>();
        const SinkDescriptorPtr sinkDescriptor = sinkNodeOperator->getSinkDescriptor();
        const DataSinkPtr dataSink = ConvertLogicalToPhysicalSink::createDataSink(sinkDescriptor);
        return createSinkOperator(dataSink);
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this operator node: " << operatorNode);
    NES_NOT_IMPLEMENTED;
}
/**
 * Translade operator node and all its children to the legacy representation.
 */
OperatorNodePtr TranslateFromLegacyPlanPhase::transform(OperatorPtr operatorPtr) {
    NES_DEBUG("TranslateFromLegacyPlanPhase: translate " << operatorPtr)
    auto operatorNode = transformIndividualOperator(operatorPtr);
    for (const OperatorPtr child: operatorPtr->getChildren()) {
        auto legacyChildOperator = transform(child);
        operatorNode->addChild(legacyChildOperator);
    }
    NES_DEBUG("TranslateFromLegacyPlanPhase: got " << operatorNode)
    return operatorNode;
}

/**
 * Translate the expression node into the corresponding user api expression of the legacy api.
 * To this end we first cast the expression node in the right subtype and then translate it.
 */
ExpressionNodePtr TranslateFromLegacyPlanPhase::transformExpression(UserAPIExpressionPtr expression) {
    if (expression-> instanceOf<LogicalExpressionNode>()) {
        // Translate logical expressions to the legacy representation
        return transformLogicalExpressions(expression);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        // Translate arithmetical expressions to the legacy representation
        return transformArithmeticalExpressions(expression);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        // Translate constant value expression node.
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        return PredicateItem(value).copy();
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        // Translate field read expression node.
        auto fieldReadExpression = expression->as<FieldAccessExpressionNode>();
        auto fieldName = fieldReadExpression->getFieldName();
        auto value = fieldReadExpression->getStamp();
        return Field(AttributeField(fieldName, value).copy()).copy();
    }
    NES_FATAL_ERROR(
        "TranslateToLegacyPhase: No transformation implemented for this expression node: " << expression->toString());
    NES_NOT_IMPLEMENTED;
}

ExpressionNodePtr TranslateFromLegacyPlanPhase::transformArithmeticalExpressions(UserAPIExpressionPtr expression) {
    if (expression-> instanceOf<AddExpressionNode>()) {
        // Translate add expression node.
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto legacyLeft = transformExpression(addExpressionNode->getLeft());
        auto legacyRight = transformExpression(addExpressionNode->getRight());
        return Predicate(BinaryOperatorType::PLUS_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<SubExpressionNode>()) {
        // Translate sub expression node.
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto legacyLeft = transformExpression(subExpressionNode->getLeft());
        auto legacyRight = transformExpression(subExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MINUS_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<MulExpressionNode>()) {
        // Translate mul expression node.
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto legacyLeft = transformExpression(mulExpressionNode->getLeft());
        auto legacyRight = transformExpression(mulExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MULTIPLY_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<DivExpressionNode>()) {
        // Translate div expression node.
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto legacyLeft = transformExpression(divExpressionNode->getLeft());
        auto legacyRight = transformExpression(divExpressionNode->getRight());
        return Predicate(BinaryOperatorType::DIVISION_OP, legacyLeft, legacyRight).copy();
    }
    NES_FATAL_ERROR(
        "TranslateToLegacyPhase: No transformation implemented for this arithmetical expression node: "
            << expression->toString());
    NES_NOT_IMPLEMENTED;
}

ExpressionNodePtr TranslateFromLegacyPlanPhase::transformLogicalExpressions(UserAPIExpressionPtr expression) {
    if (expression->instanceOf<AndExpressionNode>()) {
        // Translate and expression node.
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_AND_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<OrExpressionNode>()) {
        // Translate or expression node.
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto legacyLeft = transformExpression(orExpressionNode->getLeft());
        auto legacyRight = transformExpression(orExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_OR_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessExpressionNode>()) {
        // Translate less expression node.
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto legacyLeft = transformExpression(lessExpressionNode->getLeft());
        auto legacyRight = transformExpression(lessExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THEN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // Translate less equals expression node.
        auto andExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THEN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // Translate greater expression node.
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto legacyLeft = transformExpression(greaterExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THEN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // Translate greater equals expression node.
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto legacyLeft = transformExpression(greaterEqualsExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterEqualsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THEN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        // Translate equals expression node.
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto legacyLeft = transformExpression(equalsExpressionNode->getLeft());
        auto legacyRight = transformExpression(equalsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::EQUAL_OP, legacyLeft, legacyRight).copy();
    }
    NES_FATAL_ERROR(
        "TranslateToLegacyPhase: No transformation implemented for this logical expression node: "
            << expression->toString());
    NES_NOT_IMPLEMENTED;
}

}