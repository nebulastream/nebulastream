#include <API/UserAPIExpression.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSource.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>
#include <Operators/Impl/FilterOperator.hpp>
#include <Operators/Impl/MapOperator.hpp>
#include <Operators/Impl/SinkOperator.hpp>
#include <Operators/Impl/SourceOperator.hpp>
#include <Operators/Operator.hpp>

namespace NES {

TranslateFromLegacyPlanPhasePtr TranslateFromLegacyPlanPhase::create() {
    return std::make_shared<TranslateFromLegacyPlanPhase>();
}

/**
 * Translate operator node and all its children to the legacy representation.
 */
OperatorNodePtr TranslateFromLegacyPlanPhase::transform(OperatorPtr operatorPtr) {
    NES_DEBUG("TranslateFromLegacyPlanPhase: translate " << operatorPtr);
    auto operatorNode = transformIndividualOperator(operatorPtr);
    for (const OperatorPtr child : operatorPtr->getChildren()) {
        auto legacyChildOperator = transform(child);
        operatorNode->addChild(legacyChildOperator);
    }
    NES_DEBUG("TranslateFromLegacyPlanPhase: got " << operatorNode);
    return operatorNode;
}

/**
 * Translate the expression node into the corresponding user api expression of the legacy api.
 * To this end we first cast the expression node in the right subtype and then translate it.
 */
ExpressionNodePtr TranslateFromLegacyPlanPhase::transformToExpression(UserAPIExpressionPtr expression) {

    if (PredicatePtr predicate = std::dynamic_pointer_cast<Predicate>(expression)) {
        if (predicate->getOperatorType() == LESS_THAN_EQUAL_OP || predicate->getOperatorType() == LESS_THAN_OP || predicate->getOperatorType() == GREATER_THAN_EQUAL_OP || predicate->getOperatorType() == GREATER_THAN_OP || predicate->getOperatorType() == EQUAL_OP) {
            NES_DEBUG("TranslateFromLegacyPlanPhase: translate expression into logical logical expression");
            // Translate logical expressions to the legacy representation
            return transformLogicalExpressions(predicate);
        } else if (predicate->getOperatorType() == PLUS_OP || predicate->getOperatorType() == MINUS_OP || predicate->getOperatorType() == DIVISION_OP || predicate->getOperatorType() == MULTIPLY_OP) {
            NES_DEBUG("TranslateFromLegacyPlanPhase: translate expression into logical arithmetic expression");
            // Translate arithmetical expressions to the legacy representation
            return transformArithmeticalExpressions(predicate);
        }
        NES_THROW_RUNTIME_ERROR(
            "TranslateFromLegacyPlanPhase: No transformation implemented for this predicate: "
            + expression->toString());
        NES_NOT_IMPLEMENTED();
    } else if (FieldPtr field = std::dynamic_pointer_cast<Field>(expression)) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate expression into field access expression");
        const AttributeFieldPtr attributeField = field->getAttributeField();
        auto name = attributeField->name;
        auto dataType = attributeField->data_type;
        return FieldAccessExpressionNode::create(dataType, name);
    } else if (PredicateItemPtr predicateItem = std::dynamic_pointer_cast<PredicateItem>(expression)) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate expression into constant value expression or field access expression");
        if (predicateItem->getValue()) {
            return ConstantValueExpressionNode::create(predicateItem->getValue());
        } else if (predicateItem->getAttributeField()) {
            const AttributeFieldPtr attributeField = predicateItem->getAttributeField();
            return FieldAccessExpressionNode::create(attributeField->data_type, attributeField->name);
        }
        NES_FATAL_ERROR("TranslateFromLegacyPlanPhase: No transformation possible for input PredicateItem");
    }
    NES_THROW_RUNTIME_ERROR(
        "TranslateFromLegacyPlanPhase: No transformation implemented for this UserAPIExpression: "
        + expression->toString());
    NES_NOT_IMPLEMENTED();
}

OperatorNodePtr TranslateFromLegacyPlanPhase::transformIndividualOperator(OperatorPtr operatorPtr) {

    if (operatorPtr->getOperatorType() == SOURCE_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate operator into logical source operator");
        auto sourceOperator = std::dynamic_pointer_cast<SourceOperator>(operatorPtr);
        const DataSourcePtr dataSource = sourceOperator->getDataSourcePtr();
        const SourceDescriptorPtr sourceDescriptor = ConvertPhysicalToLogicalSource::createSourceDescriptor(dataSource);
        const LogicalOperatorNodePtr operatorNode = createSourceLogicalOperatorNode(sourceDescriptor);
        operatorNode->setId(operatorPtr->getOperatorId());
        return operatorNode;
    } else if (operatorPtr->getOperatorType() == FILTER_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate operator into logical filter operator");
        auto filterOperator = std::dynamic_pointer_cast<FilterOperator>(operatorPtr);
        const PredicatePtr predicatePtr = filterOperator->getPredicate();
        auto predicateNode = transformToExpression(predicatePtr);
        if (predicateNode == nullptr) {
            NES_THROW_RUNTIME_ERROR("TranslateFromLegacyPlanPhase: Error during translating filter operator");
        }
        const LogicalOperatorNodePtr operatorNode = createFilterLogicalOperatorNode(predicateNode);
        operatorNode->setId(operatorPtr->getOperatorId());
        return operatorNode;
    } else if (operatorPtr->getOperatorType() == MAP_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate operator into logical map operator");
        auto mapOperator = std::dynamic_pointer_cast<MapOperator>(operatorPtr);
        PredicatePtr predicate = mapOperator->getPredicate();
        ExpressionNodePtr expression = transformToExpression(predicate);
        if (expression == nullptr) {
            NES_THROW_RUNTIME_ERROR("TranslateFromLegacyPhase: Error during translating map expression");
        }
        AttributeFieldPtr field = mapOperator->getField();
        ExpressionNodePtr
            fieldAccessExpressionNodePtr = FieldAccessExpressionNode::create(field->data_type, field->name);
        FieldAssignmentExpressionNodePtr fieldAssignmentExpression =
            FieldAssignmentExpressionNode::create(fieldAccessExpressionNodePtr->as<FieldAccessExpressionNode>(),
                                                  expression);
        const LogicalOperatorNodePtr operatorNode = createMapLogicalOperatorNode(fieldAssignmentExpression);
        operatorNode->setId(operatorPtr->getOperatorId());
        return operatorNode;
    } else if (operatorPtr->getOperatorType() == SINK_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate operator into logical sink operator");
        SinkOperatorPtr sinkOperator = std::dynamic_pointer_cast<SinkOperator>(operatorPtr);
        const SinkDescriptorPtr
            sinkDescriptor = ConvertPhysicalToLogicalSink::createSinkDescriptor(sinkOperator->getDataSinkPtr());
        const LogicalOperatorNodePtr operatorNode = createSinkLogicalOperatorNode(sinkDescriptor);
        operatorNode->setId(operatorPtr->getOperatorId());
        return operatorNode;
    }
    NES_THROW_RUNTIME_ERROR(
        "TranslateFromLegacyPhase: No transformation implemented for this operator: " + operatorPtr->toString());
    NES_NOT_IMPLEMENTED();
}

ExpressionNodePtr TranslateFromLegacyPlanPhase::transformArithmeticalExpressions(PredicatePtr predicate) {
    if (predicate->getOperatorType() == PLUS_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical add expression");
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return AddExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == MINUS_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical subtraction expression");
        // Translate sub expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return SubExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == MULTIPLY_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical multiply expression");
        // Translate mul expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return MulExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == DIVISION_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical divide expression");
        // Translate div expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return DivExpressionNode::create(left, right);
    }
    NES_THROW_RUNTIME_ERROR(
        "TranslateFromLegacyPlanPhase: No transformation implemented for this arithmetical predicate: "
        + predicate->toString());
    NES_NOT_IMPLEMENTED();
}

ExpressionNodePtr TranslateFromLegacyPlanPhase::transformLogicalExpressions(PredicatePtr predicate) {
    if (predicate->getOperatorType() == LOGICAL_AND_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical and expression");
        // Translate and expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return AndExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == LOGICAL_OR_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical or expression");
        // Translate or expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return OrExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == LESS_THAN_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical less than expression");
        // Translate less expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return LessExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == LESS_THAN_EQUAL_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical less than equal expression");
        // Translate less equals expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return LessEqualsExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == GREATER_THAN_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical greater than expression");
        // Translate greater expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return GreaterExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == GREATER_THAN_EQUAL_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical greater than equal expression");
        // Translate greater equals expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return GreaterEqualsExpressionNode::create(left, right);
    } else if (predicate->getOperatorType() == EQUAL_OP) {
        NES_DEBUG("TranslateFromLegacyPlanPhase: translate predicate into logical equal expression");
        // Translate equals expression node.
        auto right = transformToExpression(predicate->getRight());
        auto left = transformToExpression(predicate->getLeft());
        return EqualsExpressionNode::create(left, right);
    }
    NES_THROW_RUNTIME_ERROR(
        "TranslateFromLegacyPlanPhase: No transformation implemented for this Physical expression node: "
        + predicate->toString());
}

}// namespace NES