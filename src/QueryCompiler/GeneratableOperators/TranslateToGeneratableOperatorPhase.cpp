
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMapOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMergeOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableWindowOperator.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

TranslateToGeneratableOperatorPhasePtr TranslateToGeneratableOperatorPhase::create() {
    return std::shared_ptr<TranslateToGeneratableOperatorPhase>();
}

TranslateToGeneratableOperatorPhase::TranslateToGeneratableOperatorPhase() {
}

OperatorNodePtr TranslateToGeneratableOperatorPhase::transformIndividualOperator(OperatorNodePtr operatorNode, OperatorNodePtr generatableParentOperator) {

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        // Translate Source operator node.
        auto scanOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto childOperator = GeneratableScanOperator::create(scanOperator->getOutputSchema());
        generatableParentOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto childOperator = GeneratableFilterOperator::create(operatorNode->as<FilterLogicalOperatorNode>());
        generatableParentOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        auto childOperator = GeneratableMapOperator::create(operatorNode->as<MapLogicalOperatorNode>());
        generatableParentOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        auto scanOperator = GeneratableScanOperator::create(operatorNode->getOutputSchema());
        generatableParentOperator->addChild(scanOperator);
        auto childOperator = GeneratableMergeOperator::create(operatorNode->as<MergeLogicalOperatorNode>());
        scanOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto childOperator = GeneratableSinkOperator::create(operatorNode->as<SinkLogicalOperatorNode>());
        return childOperator;
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        auto scanOperator = GeneratableScanOperator::create(operatorNode->getOutputSchema());
        generatableParentOperator->addChild(scanOperator);
        auto windowOperator = GeneratableWindowOperator::create(operatorNode->as<WindowLogicalOperatorNode>());
        scanOperator->addChild(windowOperator);
        return windowOperator;
    }
    NES_FATAL_ERROR("TranslateToGeneratableOperatorPhase: No transformation implemented for this operator node: " << operatorNode);
    NES_NOT_IMPLEMENTED();
}

OperatorNodePtr TranslateToGeneratableOperatorPhase::transform(OperatorNodePtr operatorNode, OperatorNodePtr legacyParent) {
    NES_DEBUG("TranslateToGeneratableOperatorPhase: translate " << operatorNode);
    auto legacyOperator = transformIndividualOperator(operatorNode, legacyParent);

    for (const NodePtr& child : operatorNode->getChildren()) {
        auto generatableOperator = transform(child->as<OperatorNode>(), legacyOperator);
    }
    NES_DEBUG("TranslateToLegacyPhase: got " << legacyOperator);
    return legacyOperator;
}

}// namespace NES