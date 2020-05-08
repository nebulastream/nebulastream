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
#include <Operators/Impl/FilterOperator.hpp>
#include <Operators/Impl/MapOperator.hpp>
#include <API/UserAPIExpression.hpp>

namespace NES {

TranslateFromLegacyPlanPhasePtr TranslateFromLegacyPlanPhase::create() {
    return std::make_shared<TranslateFromLegacyPlanPhase>();
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
ExpressionNodePtr TranslateFromLegacyPlanPhase::transformToExpression(UserAPIExpressionPtr expression) {

    if (PredicatePtr predicate = std::dynamic_pointer_cast<Predicate>(expression)) {
        if (predicate->getOperatorType() == LESS_THEN_EQUAL_OP || predicate->getOperatorType() == LESS_THEN_OP ||
            predicate->getOperatorType() == GREATER_THEN_EQUAL_OP || predicate->getOperatorType() == GREATER_THEN_OP ||
            predicate->getOperatorType() == EQUAL_OP) {
            // Translate logical expressions to the legacy representation
            return transformLogicalExpressions(predicate);
        } else if (predicate->getOperatorType() == PLUS_OP || predicate->getOperatorType() == MINUS_OP ||
            predicate->getOperatorType() == DIVISION_OP || predicate->getOperatorType() == MULTIPLY_OP) {
            // Translate arithmetical expressions to the legacy representation
            return transformArithmeticalExpressions(predicate);
        }
        NES_FATAL_ERROR(
            "TranslateFromLegacyPlanPhase: No transformation implemented for this predicate: " << expression->toString());
        NES_NOT_IMPLEMENTED;
    } else if (FieldPtr field = std::dynamic_pointer_cast<Field>(expression)) {

        const AttributeFieldPtr attributeField = field->getAttributeField();
        auto name = attributeField->name;
        auto dataType = attributeField->data_type;
        return FieldAccessExpressionNode::create(dataType, name);
    } else if (PredicateItemPtr predicateItem = std::dynamic_pointer_cast<PredicateItem>(expression)) {
        return ConstantValueExpressionNode::create(predicateItem->getValue());
    }
    NES_FATAL_ERROR(
        "TranslateFromLegacyPlanPhase: No transformation implemented for this UserAPIExpression: " << expression->toString());
    NES_NOT_IMPLEMENTED;
}

OperatorNodePtr TranslateFromLegacyPlanPhase::transformIndividualOperator(OperatorPtr operatorPtr) {

    if (operatorPtr->getOperatorType() == SOURCE_OP) {
        // Translate Source operator node.
        auto sourceOperator = std::dynamic_pointer_cast<SourceOperator>(operatorPtr);
        const DataSourcePtr dataSource = sourceOperator->getDataSourcePtr();
        return ConvertPhysicalToLogicalSource::createDataSource(dataSource);
    } else if (operatorPtr->getOperatorType() == FILTER_OP) {
        // Translate filter operator node.
        auto filterOperator = std::dynamic_pointer_cast<FilterOperator>(operatorPtr);
        const PredicatePtr predicatePtr = filterOperator->getPredicate();
        auto predicateNode = transformToExpression(predicatePtr);
        if (predicateNode == nullptr) {
            NES_FATAL_ERROR("TranslateFromLegacyPlanPhase: Error during translating filter operator");
        }
        return createFilterLogicalOperatorNode(predicateNode);
    } else if (operatorPtr->getOperatorType() == MAP_OP) {
        // Translate map operator node.

        auto filterOperator = std::dynamic_pointer_cast<MaFilterOperator>(operatorPtr);

        auto mapOperatorNode = operatorNode->as<MapLogicalOperatorNode>();
        auto mapExpression = mapOperatorNode->getMapExpression();
        // Translate to the legacy representation
        auto legacyFieldAccess = transformToExpression(mapExpression->getField());
        auto legacyAssignment = transformToExpression(mapExpression->getAssignment());
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

ExpressionNodePtr TranslateFromLegacyPlanPhase::transformArithmeticalExpressions(PredicatePtr predicate) {
    if (predicate->getOperatorType() == PLUS_OP) {
        // Translate add expression node.
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto legacyLeft = transformToExpression(addExpressionNode->getLeft());
        auto legacyRight = transformToExpression(addExpressionNode->getRight());
        return Predicate(BinaryOperatorType::PLUS_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == MINUS_OP) {
        // Translate sub expression node.
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto legacyLeft = transformToExpression(subExpressionNode->getLeft());
        auto legacyRight = transformToExpression(subExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MINUS_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == MULTIPLY_OP) {
        // Translate mul expression node.
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto legacyLeft = transformToExpression(mulExpressionNode->getLeft());
        auto legacyRight = transformToExpression(mulExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MULTIPLY_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == DIVISION_OP) {
        // Translate div expression node.
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto legacyLeft = transformToExpression(divExpressionNode->getLeft());
        auto legacyRight = transformToExpression(divExpressionNode->getRight());
        return Predicate(BinaryOperatorType::DIVISION_OP, legacyLeft, legacyRight).copy();
    }
    NES_FATAL_ERROR(
        "TranslateFromLegacyPlanPhase: No transformation implemented for this arithmetical predicate: "
            << predicate->toString());
    NES_NOT_IMPLEMENTED;
}

ExpressionNodePtr TranslateFromLegacyPlanPhase::transformLogicalExpressions(PredicatePtr predicate) {
    if (predicate->getOperatorType() == LOGICAL_AND_OP) {
        // Translate and expression node.
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto expressionLeft = transformToExpression(predicate->getLeft());
        auto expressionRight = transformToExpression(predicate->getRight());
        return AndExpressionNode::create(expressionLeft, expressionRight);
    } else if (predicate->getOperatorType() == LOGICAL_OR_OP) {
        // Translate or expression node.
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto legacyLeft = transformToExpression(orExpressionNode->getLeft());
        auto legacyRight = transformToExpression(orExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_OR_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == LESS_THEN_OP) {
        // Translate less expression node.
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto legacyLeft = transformToExpression(lessExpressionNode->getLeft());
        auto legacyRight = transformToExpression(lessExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THEN_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == LESS_THEN_EQUAL_OP) {
        // Translate less equals expression node.
        auto andExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto legacyLeft = transformToExpression(andExpressionNode->getLeft());
        auto legacyRight = transformToExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THEN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == GREATER_THEN_OP) {
        // Translate greater expression node.
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto legacyLeft = transformToExpression(greaterExpressionNode->getLeft());
        auto legacyRight = transformToExpression(greaterExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THEN_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == GREATER_THEN_EQUAL_OP) {
        // Translate greater equals expression node.
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto legacyLeft = transformToExpression(greaterEqualsExpressionNode->getLeft());
        auto legacyRight = transformToExpression(greaterEqualsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THEN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (predicate->getOperatorType() == EQUAL_OP) {
        // Translate equals expression node.
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto legacyLeft = transformToExpression(equalsExpressionNode->getLeft());
        auto legacyRight = transformToExpression(equalsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::EQUAL_OP, legacyLeft, legacyRight).copy();
    }
    NES_FATAL_ERROR(
        "TranslateFromLegacyPhase: No transformation implemented for this physical expression node: "
            << predicate->toString());
    NES_NOT_IMPLEMENTED;
}

}