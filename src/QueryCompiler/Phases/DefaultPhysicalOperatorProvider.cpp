#include <Operators/AbstractOperators/Arity/BinaryOperatorNode.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/DefaultPhysicalOperatorProvider.hpp>
namespace NES {
namespace QueryCompilation {

PhysicalOperatorProviderPtr DefaultPhysicalOperatorProvider::create() {
    return std::make_shared<DefaultPhysicalOperatorProvider>();
}

bool DefaultPhysicalOperatorProvider::isDemulticast(LogicalOperatorNodePtr operatorNode) {
    return operatorNode->getParents().size() > 1;
}

void DefaultPhysicalOperatorProvider::insertDemulticastOperatorsBefore(LogicalOperatorNodePtr operatorNode) {
    auto demultiplexOperator = PhysicalOperators::PhysicalDemultiplexOperator::create();
    demultiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndParentNodes(demultiplexOperator);
}

void DefaultPhysicalOperatorProvider::insertMulticastOperatorsAfter(LogicalOperatorNodePtr operatorNode) {
    auto multiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create();
    multiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndChildNodes(multiplexOperator);
}

void DefaultPhysicalOperatorProvider::lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {
    if (isDemulticast(operatorNode)) {
        insertDemulticastOperatorsBefore(operatorNode);
    }

    if (operatorNode->isUnaryOperator()) {
        lowerUnaryOperator(queryPlan, operatorNode);
    } else if (operatorNode->isBinaryOperator()) {
        lowerBinaryOperator(queryPlan, operatorNode);
    }
}

void DefaultPhysicalOperatorProvider::lowerUnaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {

    // If a unary operator has more then one parent, we introduce a implicit multiplex operator before.
    if (operatorNode->getChildren().size() > 1) {
        insertMulticastOperatorsAfter(operatorNode);
    }

    NES_ASSERT(operatorNode->getParents().size() <= 1, "A unary operator should only have at most one parent.");

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        auto logicalSourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto physicalSourceOperator =
            PhysicalOperators::PhysicalSourceOperator::create(logicalSourceOperator->getSourceDescriptor());
        operatorNode->replace(physicalSourceOperator);
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto logicalSinkOperator = operatorNode->as<SinkLogicalOperatorNode>();
        auto physicalSinkOperator = PhysicalOperators::PhysicalSinkOperator::create(logicalSinkOperator->getSinkDescriptor());
        operatorNode->replace(physicalSinkOperator);
        queryPlan->replaceRootOperator(logicalSinkOperator, physicalSinkOperator);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto physicalFilterOperator = PhysicalOperators::PhysicalFilterOperator::create(filterOperator->getPredicate());
        operatorNode->replace(physicalFilterOperator);
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        lowerWindowOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        lowerWatermarkAssignmentOperator(queryPlan, operatorNode);
    } else {
        NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {

    if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        lowerUnionOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        lowerJoinOperator(queryPlan, operatorNode);
    } else {
        NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerUnionOperator(QueryPlanPtr, LogicalOperatorNodePtr operatorNode) {
    auto unaryOperator = operatorNode->as<UnionLogicalOperatorNode>();
    auto physicalMultiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create();
    operatorNode->replace(physicalMultiplexOperator);
}

OperatorNodePtr DefaultPhysicalOperatorProvider::getJoinBuildInputOperator(JoinLogicalOperatorNodePtr joinOperator,
                                                                           std::vector<OperatorNodePtr> children) {
    NES_ASSERT(!children.empty(), "Their should be children for operator " << joinOperator->toString());
    if (children.size() > 1) {
        auto demultiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create();
        demultiplexOperator->setOutputSchema(joinOperator->getOutputSchema());
        demultiplexOperator->addParent(joinOperator);
        for (auto child : children) {
            child->removeParent(joinOperator);
            child->addParent(demultiplexOperator);
        }
        return demultiplexOperator;
    }
    return children[0];
}

void DefaultPhysicalOperatorProvider::lowerJoinOperator(QueryPlanPtr, LogicalOperatorNodePtr operatorNode) {
    auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();

    auto leftInputOperator = getJoinBuildInputOperator(joinOperator, joinOperator->getLeftOperators());
    auto leftJoinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(joinOperator->getJoinDefinition());
    leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);

    auto rightInputOperator = getJoinBuildInputOperator(joinOperator, joinOperator->getRightOperators());
    auto rightJoinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(joinOperator->getJoinDefinition());
    rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);

    auto joinSink = PhysicalOperators::PhysicalJoinSinkOperator::create(joinOperator->getJoinDefinition());
    operatorNode->replace(joinSink);
}

void DefaultPhysicalOperatorProvider::lowerWatermarkAssignmentOperator(QueryPlanPtr, LogicalOperatorNodePtr operatorNode) {
    auto logicalWatermarkAssignment = operatorNode->as<WatermarkAssignerLogicalOperatorNode>();
    auto physicalWatermarkAssignment = PhysicalOperators::PhysicalWatermarkAssignmentOperator::create(
        logicalWatermarkAssignment->getWatermarkStrategyDescriptor());
    operatorNode->replace(physicalWatermarkAssignment);
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(QueryPlanPtr, LogicalOperatorNodePtr operatorNode) {
    if (operatorNode->instanceOf<CentralWindowOperator>()) {
        // Translate a central window operator in -> SlicePreAggregationOperator -> WindowSinkOperator
        auto centralWindowOperator = operatorNode->as<CentralWindowOperator>();
        auto windowDefinition = centralWindowOperator->getWindowDefinition();
        auto preAggregationOperator = PhysicalOperators::PhysicalSlicePreAggregationOperator::create(windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);
        auto windowSink = PhysicalOperators::PhysicalWindowSinkOperator::create(windowDefinition);
        operatorNode->replace(windowSink);
    } else if (operatorNode->instanceOf<SliceCreationOperator>()) {
        // Translate a slice creation operator in -> SlicePreAggregationOperator -> SliceSinkOperator
        auto sliceCreationOperator = operatorNode->as<SliceCreationOperator>();
        auto windowDefinition = sliceCreationOperator->getWindowDefinition();
        auto preAggregationOperator = PhysicalOperators::PhysicalSlicePreAggregationOperator::create(windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);
        auto sliceSink = PhysicalOperators::PhysicalSliceSinkOperator::create(windowDefinition);
        operatorNode->replace(sliceSink);
    } else if (operatorNode->instanceOf<SliceMergingOperator>()) {
        // Translate a slice merging operator in -> SliceMergingOperator -> SliceSinkOperator
        auto sliceMergingOperator = operatorNode->as<SliceMergingOperator>();
        auto windowDefinition = sliceMergingOperator->getWindowDefinition();
        auto physicalSliceMergingOperator = PhysicalOperators::PhysicalSliceMergingOperator::create(windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(physicalSliceMergingOperator);
        auto sliceSink = PhysicalOperators::PhysicalSliceSinkOperator::create(windowDefinition);
        operatorNode->replace(sliceSink);
    } else if (operatorNode->instanceOf<WindowComputationOperator>()) {
        // Translate a window computation operator in -> PhysicalSliceMergingOperator -> PhysicalWindowSinkOperator
        auto windowComputation = operatorNode->as<WindowComputationOperator>();
        auto windowDefinition = windowComputation->getWindowDefinition();
        auto physicalSliceMergingOperator = PhysicalOperators::PhysicalSliceMergingOperator::create(windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(physicalSliceMergingOperator);
        auto sliceSink = PhysicalOperators::PhysicalWindowSinkOperator::create(windowDefinition);
        operatorNode->replace(sliceSink);
    } else {
        NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

}// namespace QueryCompilation
}// namespace NES