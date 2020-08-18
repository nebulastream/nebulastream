#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Operators/Operator.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <utility>
namespace NES {

TranslateToLegacyPlanPhasePtr TranslateToLegacyPlanPhase::create() {
    return std::make_shared<TranslateToLegacyPlanPhase>();
}

TranslateToLegacyPlanPhase::TranslateToLegacyPlanPhase() {}

OperatorPtr TranslateToLegacyPlanPhase::transformIndividualOperator(OperatorNodePtr operatorNode, NodeEnginePtr nodeEngine, OperatorPtr lagacyParent) {
    OperatorPtr legacyOperator;
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        // Translate Source operator node.
        auto sourceNodeOperator = operatorNode->as<SourceLogicalOperatorNode>();
        const SourceDescriptorPtr sourceDescriptor = sourceNodeOperator->getSourceDescriptor();
        const DataSourcePtr dataSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, nodeEngine);
        legacyOperator = createSourceOperator(dataSource);
        legacyOperator->setOperatorId(operatorNode->getId());
        lagacyParent->addChild(legacyOperator);
        legacyOperator->setParent(lagacyParent);
        return legacyOperator;
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        // Translate filter operator node.
        auto filterNodeOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto legacyExpression = transformExpression(filterNodeOperator->getPredicate());
        // we can assume that the legacy expression is always a predicate.
        auto legacyPredicate = std::dynamic_pointer_cast<Predicate>(legacyExpression);
        if (legacyPredicate == nullptr) {
            NES_FATAL_ERROR("TranslateToLegacyPhase: Error during translating filter expression");
        }
        legacyOperator = createFilterOperator(legacyPredicate);
        legacyOperator->setOperatorId(operatorNode->getId());
        lagacyParent->addChild(legacyOperator);
        legacyOperator->setParent(lagacyParent);
        return legacyOperator;
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        // Translate map operator node.
        auto mapOperatorNode = operatorNode->as<MapLogicalOperatorNode>();
        auto mapExpression = mapOperatorNode->getMapExpression();
        // Translate to the legacy representation
        auto legacyFieldAccess = transformExpression(mapExpression->getField());
        auto legacyAssignment = transformExpression(mapExpression->getAssignment());
        // Cast to the proper type
        auto legacyPredicate = std::dynamic_pointer_cast<UserAPIExpression>(legacyAssignment);
        auto legacyField = std::dynamic_pointer_cast<PredicateItem>(legacyFieldAccess);
        if (legacyPredicate == nullptr || legacyField == nullptr) {
            NES_FATAL_ERROR("TranslateToLegacyPhase: Error during translating map expression");
        }
        // Create legacy map operator
        legacyOperator = createMapOperator(legacyField->getAttributeField(), legacyPredicate);
        legacyOperator->setOperatorId(operatorNode->getId());
        lagacyParent->addChild(legacyOperator);
        legacyOperator->setParent(lagacyParent);
        return legacyOperator;
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        // Translate merge operator node.
        auto mergeOperatorNode = operatorNode->as<MergeLogicalOperatorNode>();
        // Create legacy merge operator
        const SchemaPtr schema = mergeOperatorNode->getOutputSchema();
        legacyOperator = createMergeOperator(schema);
        legacyOperator->setOperatorId(operatorNode->getId());
        lagacyParent->addChild(legacyOperator);
        legacyOperator->setParent(lagacyParent);
        return legacyOperator;
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        //         Translate sink operator node.
        auto sinkNodeOperator = operatorNode->as<SinkLogicalOperatorNode>();
        const SinkDescriptorPtr sinkDescriptor = sinkNodeOperator->getSinkDescriptor();
        const SchemaPtr schema = sinkNodeOperator->getOutputSchema();
        const DataSinkPtr dataSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor, nodeEngine);
        legacyOperator = createSinkOperator(dataSink);
        legacyOperator->setOperatorId(operatorNode->getId());
        return legacyOperator;
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        //         Translate window operator node.
        auto windowOperator = operatorNode->as<WindowLogicalOperatorNode>();

        auto legacyWindowScan = createWindowScanOperator(windowOperator->getOutputSchema());
        // todo both the window operator and the window scan get teh same ID. This should not be a problem.
        legacyWindowScan->setOperatorId(operatorNode->getId());
        lagacyParent->addChild(legacyWindowScan);
        legacyWindowScan->setParent(lagacyParent);

        auto legacyWindowOperator = createWindowOperator(windowOperator->getWindowDefinition());
        legacyWindowOperator->setOperatorId(operatorNode->getId());

        legacyWindowScan->addChild(legacyWindowOperator);
        legacyWindowOperator->setParent(legacyWindowScan);

        return legacyWindowOperator;
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this operator node: " << operatorNode);
    NES_NOT_IMPLEMENTED();
}
/**
 * Translade operator node and all its children to the legacy representation.
 */
OperatorPtr TranslateToLegacyPlanPhase::transform(OperatorNodePtr operatorNode, NodeEnginePtr nodeEngine, OperatorPtr legacyParent) {
    NES_DEBUG("TranslateToLegacyPhase: translate " << operatorNode);
    auto legacyOperator = transformIndividualOperator(operatorNode, nodeEngine, legacyParent);
    for (const NodePtr& child : operatorNode->getChildren()) {
        auto legacyChildOperator = transform(child->as<OperatorNode>(), nodeEngine, legacyOperator);
    }
    NES_DEBUG("TranslateToLegacyPhase: got " << legacyOperator);
    return legacyOperator;
}

/**
 * Translate the expression node into the corresponding user api expression of the legacy api.
 * To this end we first cast the expression node in the right subtype and then translate it.
 */
UserAPIExpressionPtr TranslateToLegacyPlanPhase::transformExpression(ExpressionNodePtr expression) {
    if (expression->instanceOf<LogicalExpressionNode>()) {
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
        auto stamp = fieldReadExpression->getStamp();
        NES_DEBUG(
            "TranslateToLegacyPhase: Translate FieldAccessExpressionNode: " << expression->toString());
        return Field(AttributeField::create(fieldName, stamp)).copy();
    }
    NES_FATAL_ERROR(
        "TranslateToLegacyPhase: No transformation implemented for this expression node: " << expression->toString());
    NES_NOT_IMPLEMENTED();
    ;
}

UserAPIExpressionPtr TranslateToLegacyPlanPhase::transformArithmeticalExpressions(ExpressionNodePtr expression) {
    if (expression->instanceOf<AddExpressionNode>()) {
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
    NES_NOT_IMPLEMENTED();
}

UserAPIExpressionPtr TranslateToLegacyPlanPhase::transformLogicalExpressions(ExpressionNodePtr expression) {
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
        return Predicate(BinaryOperatorType::LESS_THAN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // Translate less equals expression node.
        auto andExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THAN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // Translate greater expression node.
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto legacyLeft = transformExpression(greaterExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THAN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // Translate greater equals expression node.
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto legacyLeft = transformExpression(greaterEqualsExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterEqualsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THAN_EQUAL_OP, legacyLeft, legacyRight).copy();
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
    NES_NOT_IMPLEMENTED();
    ;
}

}// namespace NES